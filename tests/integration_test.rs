//! End-to-end integration tests for the first-reply path.
//!
//! These exercise `ClaudeClient::from_config` against a real `UreqHttpClient`
//! speaking to an in-process mock HTTP server. The mock server is a plain
//! `std::net::TcpListener` bound to `127.0.0.1:0` (kernel-assigned port) and
//! handles a single request/response cycle per test — enough to cover the
//! transport boundary without pulling in a third-party HTTP mock crate.
//!
//! The unit tests in `src/transport/claude.rs` already cover the request
//! building and status mapping via a fake `HttpClient`; these tests add
//! confidence that the same logic survives a real `ureq` round-trip.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use baton::config::{BatonConfig, Credential};
use baton::error::BatonError;
use baton::model::Prompt;
use baton::transport::Transport;
use baton::transport::claude::ANTHROPIC_VERSION;

/// The response body returned by a successful Claude Messages request.
const SUCCESS_BODY: &str = r#"{
    "id": "msg_int_1",
    "type": "message",
    "role": "assistant",
    "content": [{"type": "text", "text": "hello from the mock server"}],
    "stop_reason": "end_turn"
}"#;

/// A single-shot mock HTTP server bound to a kernel-assigned port on
/// `127.0.0.1`. The first request receives `status` + `body` and the
/// connection is closed. `hold_open` controls whether the connection is
/// accepted but never written to — used by the timeout test to make ureq
/// block on read until its own global timeout fires.
struct MockServer {
    base_url: String,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockServer {
    fn spawn(status: u16, body: &'static str) -> Self {
        Self::spawn_with(status, body, false)
    }

    /// Spawn a server that accepts the connection and never writes a
    /// response, so the client must rely on its own timeout.
    fn spawn_silent() -> Self {
        Self::spawn_with(0, "", true)
    }

    fn spawn_with(status: u16, body: &'static str, hold_open: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("read local_addr");
        let base_url = format!("http://{addr}");

        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                if hold_open {
                    // Drain the request so the client's `send` finishes
                    // writing, then sleep past any reasonable timeout to
                    // keep the connection open. The client must time out on
                    // its own — we never write a response.
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf);
                    thread::sleep(Duration::from_secs(30));
                    return;
                }

                // Drain the request. We don't care about its contents for the
                // status-mapping tests, but we must read it so the client's
                // `send` returns; otherwise the OS buffer fills and the
                // server-side write blocks.
                let mut buf = [0u8; 4096];
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.read(&mut buf);

                let response = format!(
                    "HTTP/1.1 {status} {}\r\n\
                     content-type: application/json\r\n\
                     content-length: {}\r\n\
                     connection: close\r\n\
                     \r\n\
                     {body}",
                    status_text(status),
                    body.len(),
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        // Take the handle so its lifetime is bounded by the test, but we
        // don't block on `join` here — the spawned thread is fine to be
        // torn down when the test process exits.
        let _ = self.handle.take();
    }
}

/// Maps a status code to the standard reason phrase used by the mock
/// response. We only need a handful, so a match keeps the surface small.
fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    }
}

fn config_for(base_url: &str, timeout_secs: u64) -> BatonConfig {
    config_for_credential(
        base_url,
        timeout_secs,
        Credential::ApiKey("test-key".to_string()),
    )
}

fn config_for_credential(base_url: &str, timeout_secs: u64, credential: Credential) -> BatonConfig {
    BatonConfig {
        credential,
        base_url: base_url.to_string(),
        model: "claude-test-model".to_string(),
        timeout: Duration::from_secs(timeout_secs),
        system_prompt: None,
    }
}

#[test]
fn happy_path_round_trip() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    let client =
        baton::transport::claude::ClaudeClient::from_config(config_for(server.base_url(), 5));

    let reply = client
        .send(&Prompt::new("hi"))
        .expect("happy path should succeed");
    assert_eq!(reply.text, "hello from the mock server");
}

#[test]
fn auth_failure_maps_to_auth_error() {
    let body =
        r#"{"type":"error","error":{"type":"authentication_error","message":"bad api key"}}"#;
    let server = MockServer::spawn(401, body);
    let client =
        baton::transport::claude::ClaudeClient::from_config(config_for(server.base_url(), 5));

    match client.send(&Prompt::new("hi")).unwrap_err() {
        BatonError::Auth(msg) => assert_eq!(msg, "bad api key"),
        other => panic!("expected Auth, got {other:?}"),
    }
}

#[test]
fn rate_limit_maps_to_rate_limited() {
    let body = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    let server = MockServer::spawn(429, body);
    let client =
        baton::transport::claude::ClaudeClient::from_config(config_for(server.base_url(), 5));

    match client.send(&Prompt::new("hi")).unwrap_err() {
        BatonError::RateLimited(msg) => assert_eq!(msg, "slow down"),
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

#[test]
fn malformed_response_maps_to_decode_error() {
    // 200 OK, but the body is not the JSON shape we expect. The client
    // should surface this as `Decode` rather than a silent empty reply.
    let server = MockServer::spawn(200, "<<<not json at all>>>");
    let client =
        baton::transport::claude::ClaudeClient::from_config(config_for(server.base_url(), 5));

    assert!(matches!(
        client.send(&Prompt::new("hi")).unwrap_err(),
        BatonError::Decode(_)
    ));
}

#[test]
fn timeout_maps_to_transport_error() {
    // Server accepts the connection and never writes a response — ureq's
    // global timeout should fire and the call should surface as
    // `Transport`, not `Decode` (which would mean the server returned an
    // empty 200 and we tried to parse it as JSON).
    let server = MockServer::spawn_silent();
    let client =
        baton::transport::claude::ClaudeClient::from_config(config_for(server.base_url(), 1));

    match client.send(&Prompt::new("hi")).unwrap_err() {
        BatonError::Transport(msg) => {
            // We don't pin the exact ureq phrasing, but the variant should
            // be `Transport` and the message should be non-empty.
            assert!(
                !msg.is_empty(),
                "transport message should describe the failure"
            );
        }
        other => panic!("expected Transport, got {other:?}"),
    }
}

/// Sanity check that the wire-level request carries the headers we expect.
/// The body bytes are already covered by the unit tests'
/// `request_uses_configured_endpoint_model_key_and_version` (which captures
/// the serialized body via the fake `HttpClient`); this integration test
/// adds confidence that the same headers survive a real `ureq` round-trip.
#[test]
fn request_carries_expected_headers() {
    use std::sync::{Arc, Mutex};

    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_for_thread = Arc::clone(&captured);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{addr}");

    let _server = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read until we've seen the end-of-headers marker. The body
            // bytes may still be in flight; we don't need them for header
            // assertions.
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            *captured_for_thread.lock().unwrap() = Some(String::from_utf8_lossy(&buf).into_owned());

            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 content-type: application/json\r\n\
                 content-length: {}\r\n\
                 connection: close\r\n\
                 \r\n\
                 {SUCCESS_BODY}",
                SUCCESS_BODY.len(),
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let client = baton::transport::claude::ClaudeClient::from_config(config_for(&base_url, 5));
    let _ = client.send(&Prompt::new("verify me"));

    let request = captured
        .lock()
        .unwrap()
        .clone()
        .expect("server should have captured the request");
    let lower = request.to_lowercase();
    assert!(
        lower.contains("post /v1/messages"),
        "request path: {request}"
    );
    assert!(
        lower.contains("x-api-key: test-key"),
        "api key header: {request}"
    );
    assert!(
        lower.contains(&format!(
            "anthropic-version: {}",
            ANTHROPIC_VERSION.to_lowercase()
        )),
        "anthropic version header: {request}"
    );
    assert!(lower.contains("content-type: application/json"));
}

/// Companion to `request_carries_expected_headers`: an OAuth-credentialed
/// client must emit `Authorization: Bearer <token>` on the wire, and must
/// not emit an `x-api-key` header. The captured raw request gives us the
/// same view of the wire the server actually saw.
#[test]
fn request_carries_bearer_auth_header_for_oauth_credential() {
    use std::sync::{Arc, Mutex};

    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_for_thread = Arc::clone(&captured);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{addr}");

    let _server = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break,
                }
            }
            *captured_for_thread.lock().unwrap() = Some(String::from_utf8_lossy(&buf).into_owned());

            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 content-type: application/json\r\n\
                 content-length: {}\r\n\
                 connection: close\r\n\
                 \r\n\
                 {SUCCESS_BODY}",
                SUCCESS_BODY.len(),
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let client = baton::transport::claude::ClaudeClient::from_config(config_for_credential(
        &base_url,
        5,
        Credential::OAuth("oauth-test-token".to_string()),
    ));
    let _ = client.send(&Prompt::new("verify me"));

    let request = captured
        .lock()
        .unwrap()
        .clone()
        .expect("server should have captured the request");
    let lower = request.to_lowercase();
    assert!(
        lower.contains("authorization: bearer oauth-test-token"),
        "bearer header missing: {request}"
    );
    assert!(
        !lower
            .lines()
            .any(|line| line.to_ascii_lowercase().starts_with("x-api-key")),
        "OAuth credential must not emit an x-api-key header: {request}"
    );
    // The other pinned headers still ride along.
    assert!(lower.contains(&format!(
        "anthropic-version: {}",
        ANTHROPIC_VERSION.to_lowercase()
    )));
    assert!(lower.contains("content-type: application/json"));
}

// ---------------------------------------------------------------------------
// `BATON_EVENT_LOG` end-to-end file I/O.
//
// The unit tests in `src/cli.rs` / `src/events.rs` stub the `EventSink` trait,
// so they never exercise `open_event_sink()` reading the env var, the
// `.create(true).append(true)` open, or the two-line emission landing in a real
// file. The library `send()` path used by the tests above emits no events at
// all — the sink wiring lives only in the private `execute_ask`/`open_event_sink`
// of `src/cli.rs`. The honest way to cover the documented end-to-end behaviour
// (and the path the README shows) is to run the compiled binary as a
// subprocess, pointed at the same in-process mock server, with `BATON_EVENT_LOG`
// set — then parse the resulting JSONL. `serde_json` is already a crate
// dependency, so no new dependency is pulled in.
// ---------------------------------------------------------------------------

/// A unique temp directory plus the `events.jsonl` path inside it. The
/// directory is removed on drop so a panicking assertion still cleans up. Keyed
/// by pid + a per-test tag so concurrently-running tests never collide.
struct TempEventLog {
    dir: PathBuf,
    file: PathBuf,
}

impl TempEventLog {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("baton-evt-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp event-log dir");
        let file = dir.join("events.jsonl");
        Self { dir, file }
    }
}

impl Drop for TempEventLog {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs the real `baton ask -p <prompt>` binary against `base_url`, returning
/// the captured process output.
///
/// The environment is set explicitly (and the OAuth credential vars removed) so
/// a developer's real shell environment cannot leak into the run. `event_log`
/// controls whether `BATON_EVENT_LOG` is set at all — `None` exercises the
/// recording-disabled path.
fn run_baton_ask(base_url: &str, prompt: &str, event_log: Option<&Path>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_baton"));
    cmd.arg("ask").arg("-p").arg(prompt);
    cmd.env("ANTHROPIC_API_KEY", "test-key");
    cmd.env("ANTHROPIC_BASE_URL", base_url);
    cmd.env("BATON_MODEL", "claude-test-model");
    cmd.env("BATON_TIMEOUT_SECS", "5");
    // Keep credential resolution deterministic regardless of the host env.
    cmd.env_remove("ANTHROPIC_AUTH_TOKEN");
    cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    match event_log {
        Some(path) => {
            cmd.env("BATON_EVENT_LOG", path);
        }
        None => {
            cmd.env_remove("BATON_EVENT_LOG");
        }
    }
    cmd.output().expect("run baton binary")
}

/// Reads a JSONL event file into one parsed `Value` per non-blank line.
fn read_jsonl(path: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).expect("read event log");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("each event line is valid JSON"))
        .collect()
}

#[test]
fn event_log_records_request_then_response_ok_to_file() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    let temp = TempEventLog::new("ok");

    let out = run_baton_ask(server.base_url(), "hello", Some(&temp.file));
    assert!(
        out.status.success(),
        "ask should succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // stdout stays "assistant text and nothing else".
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "hello from the mock server"
    );

    let lines = read_jsonl(&temp.file);
    assert_eq!(
        lines.len(),
        2,
        "exactly request + response_ok, got {lines:?}"
    );

    let request = &lines[0];
    assert_eq!(request["event"], "request");
    assert_eq!(request["schema"], "baton.exchange/v1");
    assert_eq!(request["model"], "claude-test-model");
    assert_eq!(request["base_url"], server.base_url());
    assert_eq!(request["prompt"], "hello");

    let response = &lines[1];
    assert_eq!(response["event"], "response_ok");
    assert_eq!(response["schema"], "baton.exchange/v1");
    assert_eq!(response["reply"], "hello from the mock server");
    // The timing field is present but its value is non-deterministic.
    assert!(
        response["duration_ms"].is_u64(),
        "response_ok carries a numeric duration_ms"
    );
}

#[test]
fn event_log_records_response_error_with_kind_auth_on_401() {
    let body =
        r#"{"type":"error","error":{"type":"authentication_error","message":"bad api key"}}"#;
    let server = MockServer::spawn(401, body);
    let temp = TempEventLog::new("err");

    let out = run_baton_ask(server.base_url(), "hello", Some(&temp.file));
    assert!(
        !out.status.success(),
        "an auth failure should exit non-zero"
    );

    // The error outcome is recorded even though the command failed.
    let lines = read_jsonl(&temp.file);
    assert_eq!(
        lines.len(),
        2,
        "request + response_error even on failure, got {lines:?}"
    );
    assert_eq!(lines[0]["event"], "request");

    let error = &lines[1];
    assert_eq!(error["event"], "response_error");
    assert_eq!(error["schema"], "baton.exchange/v1");
    assert_eq!(
        error["kind"], "auth",
        "401 maps to the documented `auth` kind"
    );
    assert!(
        error["message"]
            .as_str()
            .expect("message is a string")
            .contains("bad api key"),
        "the provider message is preserved: {error:?}"
    );
}

#[test]
fn no_event_file_created_when_env_unset() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    // The directory exists; the file path inside it must remain absent because
    // BATON_EVENT_LOG is never set for this run.
    let temp = TempEventLog::new("disabled");

    let out = run_baton_ask(server.base_url(), "hello", None);
    assert!(
        out.status.success(),
        "ask should succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(
        !temp.file.exists(),
        "no event file should be created when BATON_EVENT_LOG is unset"
    );
}

#[test]
fn successive_runs_append_to_event_file() {
    let temp = TempEventLog::new("append");

    // Two independent runs to the same log path. Each run gets its own
    // single-shot mock server (the mock handles one request per spawn).
    for _ in 0..2 {
        let server = MockServer::spawn(200, SUCCESS_BODY);
        let out = run_baton_ask(server.base_url(), "hello", Some(&temp.file));
        assert!(
            out.status.success(),
            "ask should succeed; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Append (not truncate): two runs accumulate two lines each. A regression
    // to `.write(true)` in `open_event_sink()` would leave only the last run's
    // two lines.
    let lines = read_jsonl(&temp.file);
    assert_eq!(
        lines.len(),
        4,
        "successive runs accumulate one trail, got {lines:?}"
    );
    assert_eq!(lines[0]["event"], "request");
    assert_eq!(lines[1]["event"], "response_ok");
    assert_eq!(lines[2]["event"], "request");
    assert_eq!(lines[3]["event"], "response_ok");
}
