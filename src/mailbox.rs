//! The file-mailbox: an addressable, crash-safe on-disk queue for `baton serve`.
//!
//! Where [`crate::participant`] answers one envelope in memory, a mailbox gives
//! that answer an *asynchronous, addressable* home on disk: a sender drops a
//! `baton.message/v1` request file into `pending/` and a `baton serve` daemon
//! picks it up later, answers it through the [`Participant`](crate::participant)
//! seam, and writes the reply to an outbox. Everything is a file — the "reach"
//! is the filesystem, not a socket.
//!
//! ## State machine
//!
//! Each message moves `pending → claimed → done`, one atomic `rename(2)` per
//! transition (a rename within a directory is atomic on a local filesystem, so
//! a reader never observes a half-written or half-moved message):
//!
//! - **deliver** — write a temp file, then `rename` it into `pending/<id>.json`.
//! - **claim** — `rename(pending → claimed)`; the rename *is* the lock. A second
//!   claimant racing for the same file loses with `ENOENT` and moves on.
//! - **complete** — `rename(claimed → done)`; `done/` doubles as the dedup
//!   ledger, so a redelivered id already in `done/` is dropped, not reprocessed.
//! - **reclaim** — on startup the sole live instance moves any `claimed/` entry
//!   a prior crash abandoned back to `pending/`, so no in-flight message is lost.
//!
//! ## Single-instance lock
//!
//! [`Mailbox::open`] takes an exclusive advisory lock ([`std::fs::File::try_lock`])
//! on a lockfile at the mailbox root; a second `serve` on the same root fails to
//! open. This is what makes [`reclaim_stale`](Mailbox::reclaim_stale) safe:
//! reclaim runs only at the start of the one live instance, so it can never move
//! a `claimed/` message another daemon is mid-`respond()` on. The lock is
//! advisory and per-host — reliable on a local filesystem, not across NFS.
//!
//! ## Delivery semantics
//!
//! Processing is **at-least-once**, not exactly-once: an abrupt kill (SIGKILL /
//! OOM / power loss) between `respond()` and `complete()` leaves the message in
//! `claimed/`, and the next start reclaims and reprocesses it — a repeat provider
//! call and possibly a second response. Response files are keyed by the *request*
//! id (see [`deliver_response`](Mailbox::deliver_response)) so a reprocess
//! overwrites its own not-yet-consumed reply rather than appending a second;
//! consumers still correlate/dedup on `in_reply_to` / `conversation_id`.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{BatonError, Result};
use crate::message::MessageEnvelope;

/// Name of the lockfile at the mailbox root guarding single-instance access.
const LOCK_FILE: &str = "serve.lock";

/// Process-local sequence making temp filenames unique, so two writes to the
/// same key never collide on their pre-`rename` temp file.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A file-backed mailbox rooted at a directory, holding the single-instance lock
/// for its lifetime (released when dropped).
pub struct Mailbox {
    pending: PathBuf,
    claimed: PathBuf,
    done: PathBuf,
    /// The locked lockfile handle. Held only to keep the advisory lock alive;
    /// dropping the [`Mailbox`] closes it and releases the lock.
    _lock: File,
}

/// A message claimed out of `pending/` and awaiting completion.
///
/// `key` is the request's stable id (its `message_id`, equal to the reply's
/// `in_reply_to`); it names the on-disk file across every state and keys the
/// outbox write. `request` is the parsed envelope to answer.
pub struct Claimed {
    /// Stable request id — the on-disk filename stem and the outbox key.
    pub key: String,
    /// The parsed request envelope to answer.
    pub request: MessageEnvelope,
}

impl Mailbox {
    /// Opens (creating if absent) the mailbox at `root` and acquires the
    /// exclusive single-instance lock.
    ///
    /// Fails if another live `baton serve` already holds the lock on `root`, so
    /// the caller can exit non-zero rather than run a second daemon concurrently.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let pending = root.join("pending");
        let claimed = root.join("claimed");
        let done = root.join("done");
        for dir in [root, &pending, &claimed, &done] {
            fs::create_dir_all(dir).map_err(|err| {
                BatonError::Io(format!("could not create mailbox directory {dir:?}: {err}"))
            })?;
        }

        let lock_path = root.join(LOCK_FILE);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| {
                BatonError::Io(format!("could not open mailbox lock {lock_path:?}: {err}"))
            })?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(BatonError::Io(format!(
                    "another baton serve already holds the mailbox at {root:?}"
                )));
            }
            Err(TryLockError::Error(err)) => {
                return Err(BatonError::Io(format!(
                    "could not lock mailbox {root:?}: {err}"
                )));
            }
        }

        Ok(Self {
            pending,
            claimed,
            done,
            _lock: lock,
        })
    }

    /// Delivers `envelope` into `pending/` atomically (temp file, then `rename`).
    ///
    /// The request-side counterpart of a sender dropping a message; also the
    /// entry point tests use to seed the mailbox.
    pub fn deliver(&self, envelope: &MessageEnvelope) -> Result<()> {
        let key = safe_key(&envelope.message_id)?;
        let json = serialize(envelope)?;
        atomic_write(&self.pending, &file_name(&key), &json)
    }

    /// Moves any `claimed/` entry a prior crash abandoned back to `pending/`.
    ///
    /// Safe only because [`open`](Self::open) holds the single-instance lock:
    /// with exactly one live daemon, nothing else is mid-`respond()` on a
    /// `claimed/` file, so reclaiming it cannot race a concurrent processor.
    pub fn reclaim_stale(&self) -> Result<()> {
        for entry in read_dir(&self.claimed)? {
            let path = dir_entry(entry, &self.claimed)?.path();
            let Some(key) = json_key(&path) else { continue };
            let dest = self.pending.join(file_name(&key));
            fs::rename(&path, &dest).map_err(|err| {
                BatonError::Io(format!("could not reclaim {path:?} to {dest:?}: {err}"))
            })?;
        }
        Ok(())
    }

    /// Claims the next available request, transitioning it `pending → claimed`.
    ///
    /// Returns `Ok(None)` when `pending/` holds nothing claimable. A redelivered
    /// id already in `done/` is dropped (dedup). A claimed file that will not
    /// parse is moved to `done/` and skipped with a warning, so one malformed
    /// message cannot wedge the daemon.
    pub fn claim_next(&self) -> Result<Option<Claimed>> {
        for entry in read_dir(&self.pending)? {
            let path = dir_entry(entry, &self.pending)?.path();
            let Some(key) = json_key(&path) else { continue };

            // Dedup: already answered ⇒ drop the redelivered duplicate.
            if self.done.join(file_name(&key)).exists() {
                let _ = fs::remove_file(&path);
                continue;
            }

            let claimed_path = self.claimed.join(file_name(&key));
            match fs::rename(&path, &claimed_path) {
                Ok(()) => match read_envelope(&claimed_path) {
                    Ok(request) => return Ok(Some(Claimed { key, request })),
                    Err(err) => {
                        eprintln!("warning: dropping unparseable mailbox message {key:?}: {err}");
                        let _ = fs::rename(&claimed_path, self.done.join(file_name(&key)));
                        continue;
                    }
                },
                // Lost the claim race (another pass took it) — move on.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(BatonError::Io(format!("could not claim {path:?}: {err}")));
                }
            }
        }
        Ok(None)
    }

    /// Completes a claimed message, transitioning it `claimed → done`.
    pub fn complete(&self, claimed: Claimed) -> Result<()> {
        let from = self.claimed.join(file_name(&claimed.key));
        let to = self.done.join(file_name(&claimed.key));
        fs::rename(&from, &to)
            .map_err(|err| BatonError::Io(format!("could not complete {from:?} to {to:?}: {err}")))
    }

    /// Writes `response` to `outbox`, keyed by `request_key` (the request id),
    /// atomically (temp file, then `rename`).
    ///
    /// Keying on the *request* id — not the response's own `message_id`, which is
    /// freshly minted per call and so changes on every reprocess — is what makes
    /// a reclaimed reprocess **overwrite** its earlier, not-yet-consumed reply
    /// instead of leaving a second file behind.
    pub fn deliver_response(
        &self,
        outbox: &Path,
        request_key: &str,
        response: &MessageEnvelope,
    ) -> Result<()> {
        let key = safe_key(request_key)?;
        fs::create_dir_all(outbox).map_err(|err| {
            BatonError::Io(format!(
                "could not create outbox directory {outbox:?}: {err}"
            ))
        })?;
        let json = serialize(response)?;
        atomic_write(outbox, &file_name(&key), &json)
    }
}

/// The on-disk filename for a message key.
fn file_name(key: &str) -> String {
    format!("{key}.json")
}

/// Serializes an envelope to a JSON string.
fn serialize(envelope: &MessageEnvelope) -> Result<String> {
    serde_json::to_string(envelope)
        .map_err(|err| BatonError::Io(format!("could not serialize envelope: {err}")))
}

/// Reads and parses one envelope file.
fn read_envelope(path: &Path) -> Result<MessageEnvelope> {
    let data = fs::read_to_string(path)
        .map_err(|err| BatonError::Io(format!("could not read {path:?}: {err}")))?;
    serde_json::from_str(&data)
        .map_err(|err| BatonError::Decode(format!("malformed envelope in {path:?}: {err}")))
}

/// Reads a directory, mapping the open failure to a [`BatonError::Io`].
fn read_dir(dir: &Path) -> Result<fs::ReadDir> {
    fs::read_dir(dir)
        .map_err(|err| BatonError::Io(format!("could not read mailbox directory {dir:?}: {err}")))
}

/// Unwraps one directory entry, mapping the per-entry read failure to a
/// [`BatonError::Io`] that names the directory being scanned.
fn dir_entry(entry: std::io::Result<fs::DirEntry>, dir: &Path) -> Result<fs::DirEntry> {
    entry.map_err(|err| BatonError::Io(format!("could not read an entry in {dir:?}: {err}")))
}

/// The path-safe message key for a `<key>.json` file, or `None` for anything
/// else (a non-`.json` file, a temp file, or an unsafe stem — guarding against
/// path traversal via a hostile filename).
fn json_key(path: &Path) -> Option<String> {
    if path.extension().and_then(|e| e.to_str()) != Some("json") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    is_safe_key(stem).then(|| stem.to_string())
}

/// Validates that `id` is safe to use as a mailbox filename stem.
fn safe_key(id: &str) -> Result<String> {
    if is_safe_key(id) {
        Ok(id.to_string())
    } else {
        Err(BatonError::Io(format!(
            "message id is not usable as a mailbox filename: {id:?}"
        )))
    }
}

/// A key is safe iff it names no path component that could escape the mailbox.
fn is_safe_key(key: &str) -> bool {
    !key.is_empty() && key != "." && key != ".." && !key.contains(['/', '\\', '\0'])
}

/// Writes `contents` to `dir/final_name` atomically: to a hidden temp file in
/// the same directory (so the `rename` stays on one filesystem and is atomic),
/// then `rename` over the destination. The temp name is `.`-prefixed and
/// `.tmp`-suffixed so [`json_key`] never mistakes it for a message.
fn atomic_write(dir: &Path, final_name: &str, contents: &str) -> Result<()> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{final_name}.{}.{seq}.tmp", std::process::id()));
    {
        let mut file = File::create(&tmp)
            .map_err(|err| BatonError::Io(format!("could not create {tmp:?}: {err}")))?;
        file.write_all(contents.as_bytes())
            .map_err(|err| BatonError::Io(format!("could not write {tmp:?}: {err}")))?;
        file.flush()
            .map_err(|err| BatonError::Io(format!("could not flush {tmp:?}: {err}")))?;
    }
    let dest = dir.join(final_name);
    fs::rename(&tmp, &dest).map_err(|err| {
        let _ = fs::remove_file(&tmp);
        BatonError::Io(format!("could not rename {tmp:?} to {dest:?}: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MessageKind;

    /// A unique temp directory that cleans itself up on drop, mirroring the
    /// idiom in `tests/integration_test.rs`.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let mut seq = String::new();
            seq.push_str(tag);
            let path = std::env::temp_dir().join(format!(
                "baton-mailbox-{}-{}-{}",
                std::process::id(),
                TMP_SEQ.fetch_add(1, Ordering::Relaxed),
                seq
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create temp mailbox dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn request(id: &str) -> MessageEnvelope {
        MessageEnvelope::new(
            id,
            "conv-1",
            "agent-a",
            "agent-b",
            MessageKind::Request,
            "hello",
            1_700_000_000_000,
        )
    }

    fn response_for(request_id: &str) -> MessageEnvelope {
        let mut resp = MessageEnvelope::new(
            "resp-fresh-id",
            "conv-1",
            "agent-b",
            "agent-a",
            MessageKind::Response,
            "hi",
            1_700_000_000_001,
        );
        resp.in_reply_to = Some(request_id.to_string());
        resp
    }

    fn count_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| json_key(&e.path()).is_some())
                    .count()
            })
            .unwrap_or(0)
    }

    /// deliver → claim round-trips the envelope and moves it out of `pending/`.
    #[test]
    fn deliver_then_claim_round_trips() {
        let dir = TempDir::new("rt");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        mailbox.deliver(&request("m-1")).expect("deliver");

        let claimed = mailbox.claim_next().expect("claim").expect("some");
        assert_eq!(claimed.key, "m-1");
        assert_eq!(claimed.request.body, "hello");
        // Moved out of pending into claimed.
        assert_eq!(count_files(&dir.path.join("pending")), 0);
        assert_eq!(count_files(&dir.path.join("claimed")), 1);
    }

    /// An empty mailbox yields `None`.
    #[test]
    fn claim_next_none_when_empty() {
        let dir = TempDir::new("empty");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        assert!(mailbox.claim_next().expect("claim").is_none());
    }

    /// A redelivered id already in `done/` is dropped, not re-claimed.
    #[test]
    fn dedup_skips_already_done_id() {
        let dir = TempDir::new("dedup");
        let mailbox = Mailbox::open(&dir.path).expect("open");

        // Process m-1 to done.
        mailbox.deliver(&request("m-1")).expect("deliver");
        let c = mailbox.claim_next().expect("claim").expect("some");
        mailbox.complete(c).expect("complete");
        assert_eq!(count_files(&dir.path.join("done")), 1);

        // Redeliver the same id: it must not be processed again.
        mailbox.deliver(&request("m-1")).expect("redeliver");
        assert!(
            mailbox.claim_next().expect("claim").is_none(),
            "a done id is not re-claimed"
        );
        assert_eq!(
            count_files(&dir.path.join("pending")),
            0,
            "duplicate dropped"
        );
    }

    /// Sequential claims never hand out the same file twice.
    #[test]
    fn claims_are_exclusive_across_calls() {
        let dir = TempDir::new("excl");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        mailbox.deliver(&request("m-1")).expect("deliver");
        mailbox.deliver(&request("m-2")).expect("deliver");

        let first = mailbox.claim_next().expect("claim").expect("some");
        let second = mailbox.claim_next().expect("claim").expect("some");
        assert_ne!(first.key, second.key, "distinct files");
        assert!(
            mailbox.claim_next().expect("claim").is_none(),
            "nothing left after both claimed"
        );
    }

    /// `reclaim_stale` returns an abandoned `claimed/` message to `pending/`.
    #[test]
    fn reclaim_stale_returns_claimed_to_pending() {
        let dir = TempDir::new("reclaim");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        mailbox.deliver(&request("m-1")).expect("deliver");

        // Claim but never complete — simulate a crash mid-answer.
        let _abandoned = mailbox.claim_next().expect("claim").expect("some");
        assert_eq!(count_files(&dir.path.join("claimed")), 1);

        mailbox.reclaim_stale().expect("reclaim");
        assert_eq!(count_files(&dir.path.join("claimed")), 0);
        assert_eq!(count_files(&dir.path.join("pending")), 1);
        // And it can be claimed again.
        let again = mailbox.claim_next().expect("claim").expect("some");
        assert_eq!(again.key, "m-1");
    }

    /// A second `open` on a locked root refuses (single-instance guarantee).
    #[test]
    fn second_open_on_locked_root_fails() {
        let dir = TempDir::new("lock");
        let _held = Mailbox::open(&dir.path).expect("first open");
        let second = Mailbox::open(&dir.path);
        assert!(second.is_err(), "a second instance must refuse to open");
    }

    /// Two responses keyed by the same request id collapse to one outbox file —
    /// a reprocess overwrites, never appends.
    #[test]
    fn keyed_outbox_write_overwrites_not_appends() {
        let dir = TempDir::new("outbox");
        let outbox = dir.path.join("outbox");
        let mailbox = Mailbox::open(&dir.path).expect("open");

        mailbox
            .deliver_response(&outbox, "m-1", &response_for("m-1"))
            .expect("first response");
        mailbox
            .deliver_response(&outbox, "m-1", &response_for("m-1"))
            .expect("reprocessed response");

        assert_eq!(count_files(&outbox), 1, "keyed by request id ⇒ one file");
    }

    /// An unsafe message id is rejected rather than allowed to escape the root.
    #[test]
    fn unsafe_message_id_is_rejected() {
        let dir = TempDir::new("unsafe");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        assert!(mailbox.deliver(&request("../escape")).is_err());
    }
}
