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

/// Name of the cooperative-stop sentinel at the mailbox root. A `baton serve
/// --stop` drops this file; the live daemon consumes it between messages and
/// exits 0 (Option C graceful shutdown). It sits at the root — never inside
/// `pending/` — so the message scanners never mistake it for an envelope.
const STOP_FILE: &str = "serve.stop";

/// Process-local sequence making temp filenames unique, so two writes to the
/// same key never collide on their pre-`rename` temp file.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A file-backed mailbox rooted at a directory, holding the single-instance lock
/// for its lifetime (released when dropped).
pub struct Mailbox {
    pending: PathBuf,
    claimed: PathBuf,
    done: PathBuf,
    /// The cooperative-stop sentinel path (`<root>/serve.stop`), consumed by
    /// [`poll_stop`](Mailbox::poll_stop).
    stop: PathBuf,
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
            stop: root.join(STOP_FILE),
            _lock: lock,
        })
    }

    /// Checks for and consumes the cooperative-stop sentinel in one atomic step,
    /// returning whether it was present.
    ///
    /// The single `remove_file` *is* the check-and-consume: `Ok(())` means the
    /// sentinel existed (and is now gone), `NotFound` means it never did. There
    /// is no TOCTOU gap between an existence test and the removal, and — because
    /// the caller holds the single-instance lock — no other process removes it
    /// concurrently. `serve` calls this once at startup to discard any stale
    /// sentinel, then between messages so a stop is observed without ever
    /// interrupting an in-flight `respond()`.
    pub fn poll_stop(&self) -> Result<bool> {
        match fs::remove_file(&self.stop) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(BatonError::Io(format!(
                "could not consume stop sentinel {:?}: {err}",
                self.stop
            ))),
        }
    }

    /// Delivers `envelope` into `pending/` atomically (temp file, then `rename`).
    ///
    /// The request-side counterpart of a sender dropping a message; also the
    /// entry point tests use to seed the mailbox. A producer that does not hold
    /// (and must not acquire) the single-instance lock uses the free-standing
    /// [`deliver_to`] instead — both share [`deliver_into_pending`].
    pub fn deliver(&self, envelope: &MessageEnvelope) -> Result<()> {
        deliver_into_pending(&self.pending, envelope)
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

/// The outcome of a cooperative [`request_stop`].
pub enum StopRequest {
    /// A live `serve` holds the lock; the stop sentinel was dropped for it to
    /// observe between messages.
    Signalled,
    /// No live `serve` holds the lock on this root, so no sentinel was written.
    NoDaemon,
}

/// Requests a cooperative shutdown of the `serve` daemon on `root`, if one is
/// running.
///
/// A live daemon is detected by probing the single-instance lock, *not* by
/// looking for a process: if the lock is free the caller acquires (and at once
/// releases) it, which proves no daemon is running — so it drops **no** sentinel
/// and returns [`StopRequest::NoDaemon`]. This is what keeps a fresh `serve`
/// from being killed by a stale stop file: a stop is only ever written while a
/// daemon holds the lock. If the lock is held ([`TryLockError::WouldBlock`]) a
/// daemon is live, so the sentinel is written and [`StopRequest::Signalled`]
/// returned. Either outcome is a success — cooperative stop is idempotent, so a
/// supervisor's stop hook never fails just because the daemon already exited.
///
/// A root that does not exist yet (so the lockfile's parent is missing) likewise
/// means no daemon has ever run there, and so resolves to [`StopRequest::NoDaemon`]
/// rather than an error — `--stop` never creates the mailbox it is stopping.
pub fn request_stop(root: impl AsRef<Path>) -> Result<StopRequest> {
    let root = root.as_ref();
    let lock_path = root.join(LOCK_FILE);
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
    {
        Ok(lock) => lock,
        // No mailbox directory ⇒ nothing was ever served here.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StopRequest::NoDaemon);
        }
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not open mailbox lock {lock_path:?}: {err}"
            )));
        }
    };
    match lock.try_lock() {
        // Lock acquired ⇒ no live daemon; nothing to stop. Dropping `lock`
        // releases it as this returns.
        Ok(()) => Ok(StopRequest::NoDaemon),
        // Lock held ⇒ a daemon is live; drop the sentinel for it.
        Err(TryLockError::WouldBlock) => {
            atomic_write(root, STOP_FILE, "")?;
            Ok(StopRequest::Signalled)
        }
        Err(TryLockError::Error(err)) => Err(BatonError::Io(format!(
            "could not probe mailbox lock {root:?}: {err}"
        ))),
    }
}

/// Delivers `envelope` into `<root>/pending/` atomically, **without** taking the
/// single-instance lock — the producer path used by `baton send`.
///
/// A sender posts to a mailbox a `baton serve` consumer owns the lock on, so it
/// must not open a [`Mailbox`] (which would be refused while the daemon is
/// live). It creates `<root>/pending/` if absent, then uses the identical temp
/// file + `rename(2)` write as [`Mailbox::deliver`] — the atomic delivery is
/// single-sourced in [`deliver_into_pending`]. Producer and consumer never race
/// on the same file: the request lands in `pending/` and `serve` claims it out.
pub fn deliver_to(root: impl AsRef<Path>, envelope: &MessageEnvelope) -> Result<()> {
    deliver_into_pending(&root.as_ref().join("pending"), envelope)
}

/// Attempts to claim the reply for `request_key` from `outbox`, returning the
/// parsed envelope, or `Ok(None)` when no reply is present yet.
///
/// The claim is a single `rename(2)` of `<outbox>/<key>.json` to a private,
/// `.`-prefixed non-`.json` path: the rename *is* exclusive ownership. `ENOENT`
/// means either the reply has not appeared or a concurrent consumer (or a
/// reappearing reclaim-driven v2 racing an earlier await) already claimed it —
/// both resolve to `Ok(None)` so the caller keeps polling. The claim file is
/// best-effort removed after a successful read; a crash between the rename and
/// the removal leaves a `.`-prefixed orphan, which is expected and harmless —
/// [`json_key`] ignores it, so no scanner ever mistakes it for a message.
pub fn try_claim_response(outbox: &Path, request_key: &str) -> Result<Option<MessageEnvelope>> {
    let key = safe_key(request_key)?;
    let src = outbox.join(file_name(&key));
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let claim = outbox.join(format!(".{key}.{}.{seq}.claimed", std::process::id()));
    match fs::rename(&src, &claim) {
        Ok(()) => {
            let envelope = read_envelope(&claim)?;
            let _ = fs::remove_file(&claim);
            Ok(Some(envelope))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(BatonError::Io(format!(
            "could not claim response {src:?}: {err}"
        ))),
    }
}

/// Writes `envelope` into `pending` atomically, creating the directory if absent.
///
/// The single delivery implementation shared by [`Mailbox::deliver`] (locked
/// consumer seeding its own inbox) and [`deliver_to`] (lock-free producer).
fn deliver_into_pending(pending: &Path, envelope: &MessageEnvelope) -> Result<()> {
    fs::create_dir_all(pending).map_err(|err| {
        BatonError::Io(format!(
            "could not create mailbox directory {pending:?}: {err}"
        ))
    })?;
    let key = safe_key(&envelope.message_id)?;
    let json = serialize(envelope)?;
    atomic_write(pending, &file_name(&key), &json)
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

    /// The lock-free producer seeds `pending/` even while a consumer holds the
    /// single-instance lock — the whole point of `baton send` posting to a live
    /// `serve`'s inbox — and the delivered message is then claimable.
    #[test]
    fn deliver_to_posts_without_the_lock_while_a_consumer_holds_it() {
        let dir = TempDir::new("deliver-to");
        let consumer = Mailbox::open(&dir.path).expect("consumer holds the lock");

        // Producer never opens the mailbox, so it never contends for the lock.
        deliver_to(&dir.path, &request("m-1")).expect("lock-free deliver");
        assert_eq!(count_files(&dir.path.join("pending")), 1);

        let claimed = consumer.claim_next().expect("claim").expect("some");
        assert_eq!(claimed.key, "m-1");
    }

    /// `deliver_to` creates `pending/` when the mailbox root does not exist yet
    /// (a producer racing ahead of the first `serve`).
    #[test]
    fn deliver_to_creates_pending_when_absent() {
        let dir = TempDir::new("deliver-to-fresh");
        let root = dir.path.join("nested-root");
        deliver_to(&root, &request("m-1")).expect("creates pending and delivers");
        assert_eq!(count_files(&root.join("pending")), 1);
    }

    /// An unsafe message id is rejected by the lock-free producer too.
    #[test]
    fn deliver_to_rejects_unsafe_message_id() {
        let dir = TempDir::new("deliver-to-unsafe");
        assert!(deliver_to(&dir.path, &request("../escape")).is_err());
    }

    /// A present reply is claimed once: the first call returns it and renames it
    /// out of the outbox, so a second call (a concurrent await, or a reappearing
    /// v2 racing this one) sees nothing.
    #[test]
    fn try_claim_response_claims_once_then_none() {
        let dir = TempDir::new("claim");
        let outbox = dir.path.join("outbox");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        mailbox
            .deliver_response(&outbox, "m-1", &response_for("m-1"))
            .expect("seed reply");

        let first = try_claim_response(&outbox, "m-1").expect("claim");
        assert!(first.is_some(), "the reply is claimed");
        assert_eq!(first.unwrap().in_reply_to.as_deref(), Some("m-1"));

        let second = try_claim_response(&outbox, "m-1").expect("second claim");
        assert!(second.is_none(), "a claimed reply is not double-consumed");
    }

    /// An absent reply is `Ok(None)`, not an error — the poll-again signal.
    #[test]
    fn try_claim_response_absent_is_none() {
        let dir = TempDir::new("claim-absent");
        let outbox = dir.path.join("outbox");
        fs::create_dir_all(&outbox).expect("outbox");
        assert!(try_claim_response(&outbox, "m-1").expect("claim").is_none());
    }

    /// `poll_stop` is `false` when no sentinel is present.
    #[test]
    fn poll_stop_absent_is_false() {
        let dir = TempDir::new("stop-absent");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        assert!(!mailbox.poll_stop().expect("poll"), "no sentinel ⇒ false");
    }

    /// `poll_stop` returns `true` once for a present sentinel, then consumes it —
    /// a second poll is `false`, so a stop is observed exactly once.
    #[test]
    fn poll_stop_consumes_present_sentinel() {
        let dir = TempDir::new("stop-present");
        let mailbox = Mailbox::open(&dir.path).expect("open");
        fs::write(dir.path.join(STOP_FILE), "").expect("drop sentinel");

        assert!(mailbox.poll_stop().expect("poll"), "sentinel observed");
        assert!(
            !mailbox.poll_stop().expect("poll again"),
            "sentinel consumed ⇒ not observed twice"
        );
    }

    /// `request_stop` drops the sentinel for a live daemon (lock held), and the
    /// daemon then observes it via `poll_stop`.
    #[test]
    fn request_stop_signals_while_daemon_holds_lock() {
        let dir = TempDir::new("req-stop-live");
        let daemon = Mailbox::open(&dir.path).expect("daemon holds lock");

        assert!(
            matches!(
                request_stop(&dir.path).expect("stop"),
                StopRequest::Signalled
            ),
            "a live daemon is signalled"
        );
        assert!(
            daemon.poll_stop().expect("poll"),
            "the daemon observes the dropped sentinel"
        );
    }

    /// `request_stop` on an unlocked root reports `NoDaemon` and drops no
    /// sentinel — a stale stop file can never kill a later fresh `serve`.
    #[test]
    fn request_stop_no_daemon_when_unlocked() {
        let dir = TempDir::new("req-stop-dead");
        // No Mailbox open ⇒ the lock is free.
        assert!(
            matches!(
                request_stop(&dir.path).expect("stop"),
                StopRequest::NoDaemon
            ),
            "an unlocked root has no daemon to stop"
        );
        assert!(
            !dir.path.join(STOP_FILE).exists(),
            "no sentinel is written when nothing is running"
        );
    }

    /// `request_stop` on a root that does not exist yet is `NoDaemon`, not an
    /// error — a cooperative stop never creates the mailbox it is stopping.
    #[test]
    fn request_stop_no_daemon_when_root_absent() {
        let dir = TempDir::new("req-stop-absent");
        let missing = dir.path.join("never-served");
        assert!(
            matches!(request_stop(&missing).expect("stop"), StopRequest::NoDaemon),
            "a nonexistent root has no daemon to stop"
        );
        assert!(!missing.exists(), "the root is not created by --stop");
    }
}
