use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{BatonError, Result};
use crate::mailbox;
use crate::task::TaskRecord;

static SEQ: AtomicU64 = AtomicU64::new(0);

const POLL_INTERVAL_MS: u64 = 100;

/// A durable on-disk record of one session `Run` has spawned. Platform-only
/// ownership metadata remains conditional, while the record layout and
/// persistence helpers are shared by both host implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SessionRecord {
    pub(super) id: String,
    pub(super) spec: super::SessionSpec,
    pub(super) pid: u32,
    #[cfg(unix)]
    pub(super) started_at: Option<String>,
    #[cfg(windows)]
    pub(super) started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) start_epoch_secs: Option<i64>,
    #[cfg(unix)]
    #[serde(default)]
    pub(super) stderr_path: String,
    #[cfg(windows)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) job: Option<String>,
}

/// The response body for a session-start request.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StartResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<String>,
}

/// The response body for a task-start request.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TaskStartResponse {
    #[serde(default)]
    pub(super) task_id: Option<String>,
    #[serde(default)]
    pub(super) error: Option<String>,
}

/// The filesystem plumbing shared by the session and task start channels.
/// The response consumer may add a stronger transaction boundary (the
/// task acknowledgement/rollback protocol does); this type owns only the
/// common request claim and response publication mechanics.
pub(super) struct RequestChannel<'a> {
    control: &'a Path,
    requests: PathBuf,
    processing: PathBuf,
    responses: PathBuf,
}

pub(super) struct AwaitConfig {
    await_ms: u64,
    poll_ms: u64,
    no_live_error: String,
    timeout_subject: &'static str,
}

impl AwaitConfig {
    pub(super) fn new(
        await_ms: u64,
        poll_ms: u64,
        no_live_error: String,
        timeout_subject: &'static str,
    ) -> Self {
        Self {
            await_ms,
            poll_ms,
            no_live_error,
            timeout_subject,
        }
    }
}

impl<'a> RequestChannel<'a> {
    pub(super) fn new(
        control: &'a Path,
        requests: PathBuf,
        processing: PathBuf,
        responses: PathBuf,
    ) -> Self {
        Self {
            control,
            requests,
            processing,
            responses,
        }
    }

    #[rustfmt::skip]
    pub(super) fn submit<Req, Response>(
        &self,
        request_id: &str,
        request: &Req,
        is_running: impl Fn(&Path) -> Result<bool>,
        no_live_error: impl Fn(&Path) -> String,
        request_name: &str,
        await_response: impl FnOnce() -> Result<Response>,
    ) -> Result<Response>
    where
        Req: Serialize,
    {
        if !is_running(self.control)? {
            return Err(BatonError::Io(no_live_error(self.control)));
        }
        let json = serde_json::to_string(request).map_err(|err| {
            BatonError::Io(format!("could not serialize {request_name}: {err}"))
        })?;
        fs::create_dir_all(&self.requests).map_err(|err| {
            BatonError::Io(format!("could not create {:?}: {err}", self.requests))
        })?;
        mailbox::atomic_write(&self.requests, &mailbox::file_name(request_id), &json)?;
        await_response()
    }

    pub(super) fn await_response<Response, Take, IsRunning, OnNotRunning>(
        &self,
        request_id: &str,
        config: AwaitConfig,
        mut take: Take,
        is_running: IsRunning,
        mut on_not_running: OnNotRunning,
    ) -> Result<Response>
    where
        Take: FnMut() -> Result<Option<Response>>,
        IsRunning: Fn(&Path) -> Result<bool>,
        OnNotRunning: FnMut() -> Result<Option<Response>>,
    {
        let deadline = Instant::now() + Duration::from_millis(config.await_ms);
        loop {
            if let Some(response) = take()? {
                return Ok(response);
            }
            if !is_running(self.control)? {
                if let Some(response) = on_not_running()? {
                    return Ok(response);
                }
                return Err(BatonError::Io(config.no_live_error.clone()));
            }
            if Instant::now() >= deadline {
                return Err(BatonError::Io(format!(
                    "timed out waiting for baton service to start the {} ({request_id})",
                    config.timeout_subject
                )));
            }
            std::thread::sleep(Duration::from_millis(config.poll_ms));
        }
    }

    /// Returns any processing entry a crash left mid-request to its
    /// pending directory. The caller must already hold the service lock.
    pub(super) fn reclaim_stale(&self) -> Result<()> {
        let entries = match fs::read_dir(&self.processing) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {:?}: {err}",
                    self.processing
                )));
            }
        };
        fs::create_dir_all(&self.requests).map_err(|err| {
            BatonError::Io(format!("could not create {:?}: {err}", self.requests))
        })?;
        for entry in entries {
            let path = mailbox::dir_entry(entry, &self.processing)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            let dest = self.requests.join(mailbox::file_name(&key));
            fs::rename(&path, &dest)
                .map_err(|err| BatonError::Io(format!("could not reclaim {path:?}: {err}")))?;
        }
        Ok(())
    }

    /// Claims the next pending request and lets the caller handle the
    /// claimed file. Handler-specific admission and lifecycle behavior
    /// remains outside this shared filesystem loop.
    pub(super) fn process_one<Response>(
        &self,
        mut handle: impl FnMut(&str, &Path) -> Result<Option<Response>>,
    ) -> Result<Option<Response>> {
        fs::create_dir_all(&self.requests).map_err(|err| {
            BatonError::Io(format!("could not create {:?}: {err}", self.requests))
        })?;
        let entries = match fs::read_dir(&self.requests) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {:?}: {err}",
                    self.requests
                )));
            }
        };
        for entry in entries {
            let path = mailbox::dir_entry(entry, &self.requests)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            fs::create_dir_all(&self.processing).map_err(|err| {
                BatonError::Io(format!("could not create {:?}: {err}", self.processing))
            })?;
            let claimed_path = self.processing.join(mailbox::file_name(&key));
            match fs::rename(&path, &claimed_path) {
                Ok(()) => {
                    let outcome = handle(&key, &claimed_path);
                    let _ = fs::remove_file(&claimed_path);
                    return outcome;
                }
                // Lost a claim race: move on to the next entry.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(BatonError::Io(format!("could not claim {path:?}: {err}")));
                }
            }
        }
        Ok(None)
    }

    #[rustfmt::skip]
    pub(super) fn write_response<Body: Serialize>(
        &self,
        request_id: &str,
        response: &Body,
        response_name: &str,
    ) -> Result<()> {
        let json = serde_json::to_string(response).map_err(|err| {
            BatonError::Io(format!("could not serialize {response_name}: {err}"))
        })?;
        fs::create_dir_all(&self.responses).map_err(|err| {
            BatonError::Io(format!("could not create {:?}: {err}", self.responses))
        })?;
        mailbox::atomic_write(&self.responses, &mailbox::file_name(request_id), &json)
    }

    pub(super) fn reject<Body: Serialize, Response>(
        &self,
        request_id: &str,
        response: &Body,
        response_name: &str,
    ) -> Result<Option<Response>> {
        self.write_response(request_id, response, response_name)?;
        Ok(None)
    }
}

/// Shared persistence for the two durable control-plane record kinds.
/// `noun` keeps the existing kind-specific diagnostics intact.
pub(super) struct RecordStore {
    directory: PathBuf,
    noun: &'static str,
}

impl RecordStore {
    pub(super) fn new(directory: impl Into<PathBuf>, noun: &'static str) -> Self {
        Self {
            directory: directory.into(),
            noun,
        }
    }

    pub(super) fn path(&self, id: &str) -> Result<PathBuf> {
        if !mailbox::is_safe_key(id) {
            return Err(BatonError::Io(format!(
                "{} id is not usable as a filename: {id:?}",
                self.noun
            )));
        }
        Ok(self.directory.join(mailbox::file_name(id)))
    }

    pub(super) fn write<Record: Serialize>(&self, id: &str, record: &Record) -> Result<()> {
        fs::create_dir_all(&self.directory).map_err(|err| {
            BatonError::Io(format!("could not create {:?}: {err}", self.directory))
        })?;
        let json = serde_json::to_string(record).map_err(|err| {
            BatonError::Io(format!("could not serialize {} record: {err}", self.noun))
        })?;
        mailbox::atomic_write(&self.directory, &mailbox::file_name(id), &json)
    }

    pub(super) fn read<Record: DeserializeOwned>(&self, id: &str) -> Result<Option<Record>> {
        let path = self.path(id)?;
        match fs::read(&path) {
            Ok(bytes) => {
                let data = String::from_utf8(bytes).map_err(|err| {
                    BatonError::Decode(format!("malformed {} record {path:?}: {err}", self.noun))
                })?;
                serde_json::from_str(&data).map(Some).map_err(|err| {
                    BatonError::Decode(format!("malformed {} record {path:?}: {err}", self.noun))
                })
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BatonError::Io(format!("could not read {path:?}: {err}"))),
        }
    }

    pub(super) fn exists(&self, id: &str) -> Result<bool> {
        let path = self.path(id)?;
        match path.try_exists() {
            Ok(true) => Ok(true),
            Ok(false) => match fs::metadata(&self.directory) {
                Ok(metadata) if metadata.is_dir() => Ok(false),
                Ok(_) => Err(BatonError::Io(format!(
                    "could not probe {} record {path:?}: parent {:?} is not a directory",
                    self.noun, self.directory
                ))),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(err) => Err(BatonError::Io(format!(
                    "could not probe {} record parent {:?}: {err}",
                    self.noun, self.directory
                ))),
            },
            Err(err) => Err(BatonError::Io(format!("could not probe {path:?}: {err}"))),
        }
    }

    pub(super) fn remove(&self, id: &str) -> Result<()> {
        let path = self.path(id)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BatonError::Io(format!("could not remove {path:?}: {err}"))),
        }
    }

    pub(super) fn list<Record: DeserializeOwned>(&self) -> Result<Vec<Record>> {
        let entries = match fs::read_dir(&self.directory) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {:?}: {err}",
                    self.directory
                )));
            }
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &self.directory)?.path();
            let Some(key) = mailbox::json_key(&path) else {
                continue;
            };
            match self.read(&key) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => {}
                Err(BatonError::Decode(message)) => {
                    eprintln!("warning: skipping {message}");
                }
                Err(err) => return Err(err),
            }
        }
        Ok(records)
    }

    /// Filenames only, with no read/parse of any record. Used to discover
    /// newly admitted ids cheaply, without re-decoding records already
    /// known to a caller-held cache.
    pub(super) fn list_ids(&self) -> Result<Vec<String>> {
        let entries = match fs::read_dir(&self.directory) {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not read {:?}: {err}",
                    self.directory
                )));
            }
        };
        let mut ids = Vec::new();
        for entry in entries {
            let path = mailbox::dir_entry(entry, &self.directory)?.path();
            if let Some(key) = mailbox::json_key(&path) {
                ids.push(key);
            }
        }
        Ok(ids)
    }
}

// -- Control-plane paths and identifiers --------------------------------

pub(super) fn requests_dir(control: &Path) -> PathBuf {
    control.join("requests")
}

pub(super) fn processing_dir(control: &Path) -> PathBuf {
    control.join("processing")
}

pub(super) fn responses_dir(control: &Path) -> PathBuf {
    control.join("responses")
}

pub(super) fn start_channel(control: &Path) -> RequestChannel<'_> {
    RequestChannel::new(
        control,
        requests_dir(control),
        processing_dir(control),
        responses_dir(control),
    )
}

pub(super) fn task_requests_dir(control: &Path) -> PathBuf {
    control.join("task-requests")
}

pub(super) fn task_processing_dir(control: &Path) -> PathBuf {
    control.join("task-processing")
}

pub(super) fn task_responses_dir(control: &Path) -> PathBuf {
    control.join("task-responses")
}

pub(super) fn task_channel(control: &Path) -> RequestChannel<'_> {
    RequestChannel::new(
        control,
        task_requests_dir(control),
        task_processing_dir(control),
        task_responses_dir(control),
    )
}

pub(super) fn task_start_ack_dir(control: &Path) -> PathBuf {
    control.join("task-start-ack")
}

pub(super) fn tasks_dir(control: &Path) -> PathBuf {
    control.join("tasks")
}

pub(super) fn task_logs_dir(control: &Path, task_id: &str) -> PathBuf {
    control.join("task-logs").join(task_id)
}

/// Reclaims a task's captured `stdout`/`stderr` tree once nothing refers to it
/// any more. Best effort: an already-absent directory is success, and an id
/// that is not usable as a filename is refused here rather than at each call
/// site, so no caller can walk `remove_dir_all` out of `task-logs/`.
pub(super) fn remove_task_logs_dir(control: &Path, task_id: &str) {
    if !mailbox::is_safe_key(task_id) {
        return;
    }
    let _ = fs::remove_dir_all(task_logs_dir(control, task_id));
}

pub(super) fn task_start_rollback_dir(control: &Path) -> PathBuf {
    control.join("task-start-rollback")
}

pub(super) fn task_cancel_dir(control: &Path) -> PathBuf {
    control.join("task-cancel")
}

pub(super) fn sessions_dir(control: &Path) -> PathBuf {
    control.join("sessions")
}

pub(super) fn fresh_request_id() -> String {
    format!(
        "req-{}-{}-{}",
        std::process::id(),
        crate::events::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn fresh_session_id() -> String {
    format!(
        "svc-{}-{}-{}",
        std::process::id(),
        crate::events::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn fresh_task_id() -> String {
    format!(
        "task-{}-{}-{}",
        std::process::id(),
        crate::events::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn reclaim_stale_requests(control: &Path) -> Result<()> {
    start_channel(control).reclaim_stale()
}

pub(super) fn write_start_response(
    control: &Path,
    request_id: &str,
    response: &StartResponse,
) -> Result<()> {
    start_channel(control).write_response(request_id, response, "start response")
}

pub(super) fn task_records(control: &Path) -> RecordStore {
    RecordStore::new(tasks_dir(control), "task")
}

#[cfg(test)]
pub(super) fn task_record_path(control: &Path, id: &str) -> Result<PathBuf> {
    task_records(control).path(id)
}

pub(super) fn write_task_record(control: &Path, record: &TaskRecord) -> Result<()> {
    task_records(control).write(&record.id, record)
}

pub(super) fn read_task_record(control: &Path, id: &str) -> Result<Option<TaskRecord>> {
    task_records(control).read(id)
}

pub(super) fn task_record_exists(control: &Path, id: &str) -> Result<bool> {
    task_records(control).exists(id)
}

pub(super) fn remove_task_record(control: &Path, id: &str) -> Result<()> {
    task_records(control).remove(id)
}

pub(super) fn list_task_records(control: &Path) -> Result<Vec<TaskRecord>> {
    task_records(control).list()
}

pub(super) fn list_task_record_ids(control: &Path) -> Result<Vec<String>> {
    task_records(control).list_ids()
}

pub(super) fn session_records(control: &Path) -> RecordStore {
    RecordStore::new(sessions_dir(control), "session")
}

#[cfg(test)]
pub(super) fn session_record_path(control: &Path, id: &str) -> Result<PathBuf> {
    session_records(control).path(id)
}

pub(super) fn write_session_record(control: &Path, record: &SessionRecord) -> Result<()> {
    session_records(control).write(&record.id, record)
}

pub(super) fn read_session_record(control: &Path, id: &str) -> Result<Option<SessionRecord>> {
    session_records(control).read(id)
}

pub(super) fn remove_session_record(control: &Path, id: &str) -> Result<()> {
    session_records(control).remove(id)
}

pub(super) fn list_session_records(control: &Path) -> Result<Vec<SessionRecord>> {
    session_records(control).list()
}

pub(super) fn task_start_response_path(control: &Path, request_id: &str) -> Result<PathBuf> {
    if !mailbox::is_safe_key(request_id) {
        return Err(BatonError::Io(format!(
            "task start request id is not usable as a filename: {request_id:?}"
        )));
    }
    Ok(task_responses_dir(control).join(mailbox::file_name(request_id)))
}

pub(super) fn task_start_response_claim_path(control: &Path, request_id: &str) -> Result<PathBuf> {
    let response = task_start_response_path(control, request_id)?;
    let file_name = response
        .file_name()
        .expect("task-start response path has a filename")
        .to_string_lossy();
    Ok(response.with_file_name(format!(".{file_name}.claimed")))
}

pub(super) fn task_start_ack_path(control: &Path, request_id: &str) -> Result<PathBuf> {
    if !mailbox::is_safe_key(request_id) {
        return Err(BatonError::Io(format!(
            "task start request id is not usable as a filename: {request_id:?}"
        )));
    }
    Ok(task_start_ack_dir(control).join(mailbox::file_name(request_id)))
}

pub(super) fn task_start_response_boundary_exists(
    control: &Path,
    request_id: &str,
) -> Result<bool> {
    Ok(task_start_response_path(control, request_id)?.is_file()
        || task_start_response_claim_path(control, request_id)?.is_file()
        || task_start_ack_path(control, request_id)?.is_file())
}

pub(super) fn take_task_start_response_locked(
    control: &Path,
    request_id: &str,
) -> Result<Option<TaskStartResponse>> {
    let response_path = task_start_response_path(control, request_id)?;
    let claim_path = task_start_response_claim_path(control, request_id)?;

    if task_start_ack_exists(control, request_id)? {
        remove_task_start_response_files(control, request_id)?;
        return Ok(None);
    }
    restore_task_start_response_claim(control, request_id)?;
    match fs::rename(&response_path, &claim_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not claim task response {response_path:?}: {err}"
            )));
        }
    }
    let data = match fs::read_to_string(&claim_path) {
        Ok(data) => data,
        Err(err) => {
            restore_task_start_response_claim(control, request_id)?;
            return Err(BatonError::Io(format!(
                "could not read claimed task response {claim_path:?}: {err}"
            )));
        }
    };
    let response = match serde_json::from_str(&data) {
        Ok(response) => response,
        Err(err) => {
            restore_task_start_response_claim(control, request_id)?;
            return Err(BatonError::Decode(format!(
                "malformed task response {response_path:?}: {err}"
            )));
        }
    };
    mark_task_start_ack(control, request_id)?;
    wait_for_test_task_start_ack_barrier();
    let _ = fs::remove_file(&claim_path);
    Ok(Some(response))
}

pub(super) fn restore_task_start_response_claim(control: &Path, request_id: &str) -> Result<()> {
    let response_path = task_start_response_path(control, request_id)?;
    let claim_path = task_start_response_claim_path(control, request_id)?;
    if !claim_path.is_file() {
        return Ok(());
    }
    if response_path.is_file() {
        return remove_file_if_present(&claim_path, "task response claim");
    }
    match fs::rename(&claim_path, &response_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BatonError::Io(format!(
            "could not restore task response claim {claim_path:?}: {err}"
        ))),
    }
}

pub(super) fn remove_task_start_response_files(control: &Path, request_id: &str) -> Result<()> {
    let response_path = task_start_response_path(control, request_id)?;
    let claim_path = task_start_response_claim_path(control, request_id)?;
    remove_file_if_present(&response_path, "task response")?;
    remove_file_if_present(&claim_path, "task response claim")
}

pub(super) fn remove_file_if_present(path: &Path, description: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BatonError::Io(format!(
            "could not remove {description} {path:?}: {err}"
        ))),
    }
}

pub(super) fn mark_task_start_ack(control: &Path, request_id: &str) -> Result<()> {
    let dir = task_start_ack_dir(control);
    fs::create_dir_all(&dir)
        .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
    let path = mailbox::file_name(request_id);
    mailbox::atomic_write(&dir, &path, "")
}

pub(super) fn task_start_ack_exists(control: &Path, request_id: &str) -> Result<bool> {
    Ok(task_start_ack_path(control, request_id)?.is_file())
}

pub(super) fn remove_task_start_ack(control: &Path, request_id: &str) -> Result<()> {
    let path = task_start_ack_path(control, request_id)?;
    remove_file_if_present(&path, "task-start acknowledgement")
}

pub(super) fn remove_task_start_transaction(control: &Path, record: &TaskRecord) -> Result<()> {
    let Some(request_id) = record.request_id.as_deref() else {
        return Ok(());
    };
    discard_pending_task_start_request(control, request_id)?;
    remove_task_start_response_files(control, request_id)?;
    remove_task_start_ack(control, request_id)?;
    remove_task_start_rollback(control, request_id)
}

pub(super) fn list_task_start_acks(control: &Path) -> Result<Vec<String>> {
    let dir = task_start_ack_dir(control);
    let entries = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not read task-start acknowledgement directory {dir:?}: {err}"
            )));
        }
    };
    let mut request_ids = Vec::new();
    for entry in entries {
        let path = mailbox::dir_entry(entry, &dir)?.path();
        let Some(key) = mailbox::json_key(&path) else {
            continue;
        };
        request_ids.push(key);
    }
    Ok(request_ids)
}

pub(super) fn list_task_start_response_claims(control: &Path) -> Result<Vec<String>> {
    let dir = task_responses_dir(control);
    let entries = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not read task response directory {dir:?}: {err}"
            )));
        }
    };
    let mut request_ids = Vec::new();
    for entry in entries {
        let path = mailbox::dir_entry(entry, &dir)?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(response_name) = name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".claimed"))
        else {
            continue;
        };
        let response_path = dir.join(response_name);
        if let Some(request_id) = mailbox::json_key(&response_path) {
            request_ids.push(request_id);
        }
    }
    Ok(request_ids)
}

pub(super) fn task_start_response_id(
    control: &Path,
    request_id: &str,
    response: TaskStartResponse,
) -> Result<String> {
    if let Some(error) = response.error {
        return Err(BatonError::Io(error));
    }
    response.task_id.ok_or_else(|| {
        let path = task_responses_dir(control).join(mailbox::file_name(request_id));
        BatonError::Decode(format!(
            "task response {path:?} contained neither a task id nor an error"
        ))
    })
}

pub(super) fn discard_pending_task_start_request(control: &Path, request_id: &str) -> Result<()> {
    let file_name = mailbox::file_name(request_id);
    for dir in [task_requests_dir(control), task_processing_dir(control)] {
        let path = dir.join(&file_name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not discard pending task start request {path:?}: {err}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn mark_task_start_rollback(control: &Path, request_id: &str) -> Result<()> {
    let dir = task_start_rollback_dir(control);
    fs::create_dir_all(&dir)
        .map_err(|err| BatonError::Io(format!("could not create {dir:?}: {err}")))?;
    mailbox::atomic_write(&dir, &mailbox::file_name(request_id), "")
}

pub(super) fn task_start_rollback_path(control: &Path, request_id: &str) -> Result<PathBuf> {
    if !mailbox::is_safe_key(request_id) {
        return Err(BatonError::Io(format!(
            "task start request id is not usable as a filename: {request_id:?}"
        )));
    }
    Ok(task_start_rollback_dir(control).join(mailbox::file_name(request_id)))
}

pub(super) fn task_start_rollback_exists(control: &Path, request_id: &str) -> Result<bool> {
    Ok(task_start_rollback_path(control, request_id)?.is_file())
}

pub(super) fn remove_task_start_rollback(control: &Path, request_id: &str) -> Result<()> {
    let path = task_start_rollback_path(control, request_id)?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BatonError::Io(format!(
            "could not remove task start rollback marker {path:?}: {err}"
        ))),
    }
}

pub(super) fn list_task_start_rollbacks(control: &Path) -> Result<Vec<String>> {
    let dir = task_start_rollback_dir(control);
    let entries = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(BatonError::Io(format!(
                "could not read task start rollback directory {dir:?}: {err}"
            )));
        }
    };
    let mut request_ids = Vec::new();
    for entry in entries {
        let path = mailbox::dir_entry(entry, &dir)?.path();
        let Some(key) = mailbox::json_key(&path) else {
            continue;
        };
        request_ids.push(key);
    }
    Ok(request_ids)
}

#[cfg(debug_assertions)]
pub(super) fn wait_for_test_task_admission_barrier() {
    let Some(path) = std::env::var_os("BATON_TEST_TASK_ADMISSION_BARRIER") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    while path.exists() {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

#[cfg(not(debug_assertions))]
pub(super) fn wait_for_test_task_admission_barrier() {}

#[cfg(debug_assertions)]
pub(super) fn wait_for_test_task_response_phase_barrier() {
    let Some(path) = std::env::var_os("BATON_TEST_TASK_RESPONSE_PHASE_BARRIER") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    while path.exists() {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

#[cfg(not(debug_assertions))]
pub(super) fn wait_for_test_task_response_phase_barrier() {}

#[cfg(debug_assertions)]
pub(super) fn wait_for_test_task_start_ack_barrier() {
    let Some(path) = std::env::var_os("BATON_TEST_TASK_START_ACK_BARRIER") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    while path.exists() {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

#[cfg(not(debug_assertions))]
pub(super) fn wait_for_test_task_start_ack_barrier() {}

#[cfg(debug_assertions)]
pub(super) const TEST_TASK_ROLLBACK_RECONCILE_BARRIER: &str =
    "BATON_TEST_TASK_ROLLBACK_RECONCILE_BARRIER";
#[cfg(not(debug_assertions))]
pub(super) const TEST_TASK_ROLLBACK_RECONCILE_BARRIER: &str = "";

#[cfg(debug_assertions)]
pub(super) const TEST_TASK_ROLLBACK_REQUEST_BARRIER: &str =
    "BATON_TEST_TASK_ROLLBACK_REQUEST_BARRIER";
#[cfg(not(debug_assertions))]
pub(super) const TEST_TASK_ROLLBACK_REQUEST_BARRIER: &str = "";

#[cfg(debug_assertions)]
pub(super) fn wait_for_test_task_rollback_cleanup_barrier(variable: &str) {
    let Some(path) = std::env::var_os(variable) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    while path.exists() {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
    }
}

#[cfg(not(debug_assertions))]
pub(super) fn wait_for_test_task_rollback_cleanup_barrier(_variable: &str) {}

pub(super) fn write_task_start_response(
    control: &Path,
    request_id: &str,
    response: &TaskStartResponse,
) -> Result<()> {
    // This failure injection is needed by integration tests, whose
    // test-built binary has debug assertions enabled. Keep the release
    // binary free of the test environment seam.
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("BATON_TEST_TASK_START_RESPONSE_WRITE_FAILURE") {
        let path = std::path::PathBuf::from(path);
        match fs::remove_file(&path) {
            Ok(()) => {
                return Err(BatonError::Io(format!(
                    "test-injected task-start response write failure at {path:?}"
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(BatonError::Io(format!(
                    "could not consume task-start response failure marker {path:?}: {err}"
                )));
            }
        }
    }
    task_channel(control).write_response(request_id, response, "task start response")
}
