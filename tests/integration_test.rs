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
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use baton::config::{BatonConfig, Credential, DEFAULT_MAX_TOKENS};
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
    "stop_reason": "end_turn",
    "usage": {"input_tokens": 9, "output_tokens": 3}
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

    /// Spawn a server that answers every incoming connection with the same
    /// `status` + `body`, looping until the process exits. A `baton session`
    /// run opens one connection per turn (the response sets `connection:
    /// close`), so a multi-turn session needs a server that serves more than
    /// once.
    fn spawn_repeating(status: u16, body: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("read local_addr");
        let base_url = format!("http://{addr}");

        let handle = thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut stream) = conn else { break };
                // Drain the request so the client's `send` returns, then write
                // the canned response. One request/response per connection.
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
        max_tokens: DEFAULT_MAX_TOKENS,
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
    // The provider's token usage is recorded end-to-end.
    assert_eq!(response["input_tokens"], 9);
    assert_eq!(response["output_tokens"], 3);
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

// ---------------------------------------------------------------------------
// `baton session` end-to-end.
//
// The unit tests in `src/cli.rs` drive `execute_session` with in-memory buffers
// and a fake transport. This subprocess test adds confidence that the compiled
// binary parses the `session` command, reads turns from stdin until EOF, sends
// each turn over a real `ureq` round-trip, and records a `request` +
// `response_ok` pair per turn to `BATON_EVENT_LOG`.
// ---------------------------------------------------------------------------

/// Runs the real `baton session` binary against `base_url`, piping `input` to
/// its stdin (closed after writing, which the REPL sees as EOF). Mirrors the
/// deterministic environment of [`run_baton_ask`].
fn run_baton_session(
    base_url: &str,
    input: &str,
    event_log: Option<&Path>,
) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_baton"));
    cmd.arg("session");
    cmd.env("ANTHROPIC_API_KEY", "test-key");
    cmd.env("ANTHROPIC_BASE_URL", base_url);
    cmd.env("BATON_MODEL", "claude-test-model");
    cmd.env("BATON_TIMEOUT_SECS", "5");
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
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn baton session");
    child
        .stdin
        .take()
        .expect("session stdin is piped")
        .write_all(input.as_bytes())
        .expect("write session input");
    // Dropping the taken stdin (end of the statement above) closes the pipe,
    // so the REPL reads EOF and exits.
    child.wait_with_output().expect("wait for baton session")
}

#[test]
fn session_runs_multi_turn_and_records_a_pair_per_turn() {
    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let temp = TempEventLog::new("session");

    let out = run_baton_session(
        server.base_url(),
        "first turn\nsecond turn\n",
        Some(&temp.file),
    );
    assert!(
        out.status.success(),
        "session should exit 0 on EOF; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The assistant reply is printed once per turn.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let reply_count = stdout.matches("hello from the mock server").count();
    assert_eq!(
        reply_count, 2,
        "one reply printed per turn; stdout: {stdout}"
    );

    // Two turns ⇒ two request/response_ok pairs in order.
    let lines = read_jsonl(&temp.file);
    assert_eq!(
        lines.len(),
        4,
        "two turns × (request + response_ok), got {lines:?}"
    );
    assert_eq!(lines[0]["event"], "request");
    assert_eq!(lines[0]["prompt"], "first turn");
    assert_eq!(lines[1]["event"], "response_ok");
    assert_eq!(lines[2]["event"], "request");
    assert_eq!(lines[2]["prompt"], "second turn");
    assert_eq!(lines[3]["event"], "response_ok");
}

// ---------------------------------------------------------------------------
// `baton log show` / `baton log replay` end-to-end.
//
// The unit tests in `src/log.rs` / `src/cli.rs` cover `parse_jsonl`, exchange
// selection, and rendering with in-memory buffers. These subprocess tests add
// confidence that the compiled binary reads a real JSONL file from `--file`,
// renders it, and — for replay — re-sends the recorded exchange over a real
// `ureq` round-trip and appends a fresh exchange to `BATON_EVENT_LOG`.
// ---------------------------------------------------------------------------

/// Runs `baton log <args...>` with the deterministic credential environment.
/// `event_log` controls `BATON_EVENT_LOG` (the replay sink); the source log is
/// passed via `--file` in `args`.
fn run_baton_log(args: &[&str], event_log: Option<&Path>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_baton"));
    cmd.arg("log").args(args);
    cmd.env("ANTHROPIC_API_KEY", "test-key");
    cmd.env("BATON_MODEL", "claude-test-model");
    cmd.env("BATON_TIMEOUT_SECS", "5");
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
    cmd.output().expect("run baton log")
}

#[test]
fn log_show_renders_recorded_exchanges_from_file() {
    let temp = TempEventLog::new("show");
    let trail = concat!(
        r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"who won the 1998 world cup?"}"#,
        "\n",
        r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"France."}"#,
        "\n",
    );
    std::fs::write(&temp.file, trail).expect("write trail");

    let out = run_baton_log(&["show", "--file", temp.file.to_str().unwrap()], None);
    assert!(
        out.status.success(),
        "show should succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("2023-11-14T22:13:20Z"),
        "timestamp: {stdout}"
    );
    assert!(stdout.contains("claude-sonnet-4-6"), "model: {stdout}");
    assert!(
        stdout.contains("who won the 1998 world cup?"),
        "prompt: {stdout}"
    );
    assert!(stdout.contains("France."), "reply: {stdout}");
}

#[test]
fn log_show_without_source_is_usage_error() {
    // No --file and no BATON_EVENT_LOG ⇒ nothing to read ⇒ non-zero exit.
    let out = run_baton_log(&["show"], None);
    assert!(!out.status.success(), "missing source should exit non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("BATON_EVENT_LOG") || stderr.contains("--file"),
        "stderr should name the missing source: {stderr}"
    );
}

#[test]
fn log_replay_resends_last_exchange_and_appends_fresh_events() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    let source = TempEventLog::new("replay-src");
    let sink = TempEventLog::new("replay-sink");

    // The recorded request points at the mock server, so replay re-sends there.
    let trail = format!(
        concat!(
            r#"{{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-test-model","base_url":"{base}","prompt":"replay me"}}"#,
            "\n",
            r#"{{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"old reply"}}"#,
            "\n",
        ),
        base = server.base_url(),
    );
    std::fs::write(&source.file, trail).expect("write source trail");

    let out = run_baton_log(
        &["replay", "--file", source.file.to_str().unwrap()],
        Some(&sink.file),
    );
    assert!(
        out.status.success(),
        "replay should succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // stdout is the fresh reply and nothing else — same contract as `ask`.
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "hello from the mock server"
    );

    // A fresh request/response_ok pair is appended to BATON_EVENT_LOG, carrying
    // the replayed prompt.
    let lines = read_jsonl(&sink.file);
    assert_eq!(
        lines.len(),
        2,
        "replay appends one fresh exchange: {lines:?}"
    );
    assert_eq!(lines[0]["event"], "request");
    assert_eq!(lines[0]["prompt"], "replay me");
    assert_eq!(lines[1]["event"], "response_ok");
    assert_eq!(lines[1]["reply"], "hello from the mock server");
}

#[test]
fn log_replay_out_of_range_index_is_error() {
    let source = TempEventLog::new("replay-range");
    let trail = concat!(
        r#"{"event":"request","ts_ms":1,"model":"claude-test-model","base_url":"https://api.anthropic.com","prompt":"only"}"#,
        "\n",
        r#"{"event":"response_ok","ts_ms":2,"duration_ms":1,"reply":"r"}"#,
        "\n",
    );
    std::fs::write(&source.file, trail).expect("write trail");

    let out = run_baton_log(
        &[
            "replay",
            "--index",
            "5",
            "--file",
            source.file.to_str().unwrap(),
        ],
        None,
    );
    assert!(
        !out.status.success(),
        "out-of-range index should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("1..=1"),
        "stderr names the valid range: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// `baton exchange` end-to-end.
//
// The unit tests in `src/cli.rs` drive `execute_exchange` with a fake transport
// and in-memory buffers. These subprocess tests add confidence that the
// compiled binary parses the `exchange` command, reads one `baton.message/v1`
// request envelope from stdin, runs a real `ureq` round-trip, and writes one
// response envelope to stdout — including the delivered-error exit-0 contract.
// ---------------------------------------------------------------------------

/// Runs the real `baton exchange` binary against `base_url`, piping `request`
/// (a JSON envelope) to its stdin. Mirrors the deterministic environment of
/// [`run_baton_ask`].
fn run_baton_exchange(base_url: &str, request: &str) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_baton"));
    cmd.arg("exchange");
    cmd.env("ANTHROPIC_API_KEY", "test-key");
    cmd.env("ANTHROPIC_BASE_URL", base_url);
    cmd.env("BATON_MODEL", "claude-test-model");
    cmd.env("BATON_TIMEOUT_SECS", "5");
    cmd.env_remove("ANTHROPIC_AUTH_TOKEN");
    cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    cmd.env_remove("BATON_EVENT_LOG");
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn baton exchange");
    child
        .stdin
        .take()
        .expect("exchange stdin is piped")
        .write_all(request.as_bytes())
        .expect("write exchange request");
    child.wait_with_output().expect("wait for baton exchange")
}

/// A well-formed `request` envelope, addressed a→b, on conversation `conv-1`.
const REQUEST_ENVELOPE: &str = r#"{
    "schema": "baton.message/v1",
    "message_id": "m-1",
    "conversation_id": "conv-1",
    "from": "agent-a",
    "to": "agent-b",
    "in_reply_to": null,
    "kind": "request",
    "body": "hi",
    "ts_ms": 1700000000000,
    "exchange": null
}"#;

#[test]
fn exchange_round_trips_a_response_envelope() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    let out = run_baton_exchange(server.base_url(), REQUEST_ENVELOPE);

    assert!(
        out.status.success(),
        "a successful exchange should exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.lines().count(), 1, "exactly one envelope: {stdout}");

    let resp: serde_json::Value = serde_json::from_str(stdout.trim()).expect("stdout is JSON");
    assert_eq!(resp["schema"], "baton.message/v1");
    assert_eq!(resp["kind"], "response");
    assert_eq!(resp["conversation_id"], "conv-1");
    assert_eq!(resp["in_reply_to"], "m-1");
    // Addressing swaps.
    assert_eq!(resp["from"], "agent-b");
    assert_eq!(resp["to"], "agent-a");
    assert_eq!(resp["body"], "hello from the mock server");
    // Fresh message id, distinct from the request.
    assert_ne!(resp["message_id"], "m-1");
    // The provider call is wrapped in-band, carrying #37 token usage.
    assert_eq!(resp["exchange"]["schema"], "baton.exchange/v1");
    assert_eq!(
        resp["exchange"]["exchange"]["outcome"]["event"],
        "response_ok"
    );
    assert_eq!(resp["exchange"]["exchange"]["outcome"]["input_tokens"], 9);
    assert_eq!(resp["exchange"]["exchange"]["outcome"]["output_tokens"], 3);
}

#[test]
fn exchange_delivers_provider_error_as_envelope_and_exits_zero() {
    let body =
        r#"{"type":"error","error":{"type":"authentication_error","message":"bad api key"}}"#;
    let server = MockServer::spawn(401, body);
    let out = run_baton_exchange(server.base_url(), REQUEST_ENVELOPE);

    // Delivered-error contract: a provider failure is a *delivered response*,
    // so the process still exits 0 with the error envelope on stdout.
    assert!(
        out.status.success(),
        "a delivered provider error still exits 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let resp: serde_json::Value = serde_json::from_str(stdout.trim()).expect("stdout is JSON");
    assert_eq!(resp["kind"], "error");
    assert_eq!(resp["in_reply_to"], "m-1");
    assert_eq!(
        resp["exchange"]["exchange"]["outcome"]["event"],
        "response_error"
    );
    assert_eq!(resp["exchange"]["exchange"]["outcome"]["kind"], "auth");
}

#[test]
fn exchange_malformed_request_exits_non_zero_with_empty_stdout() {
    // No provider call is made, so no server is needed. A malformed request
    // envelope is a usage error: non-zero exit, a stderr diagnostic, and
    // *nothing* on stdout (the response is emitted only after a completed
    // exchange).
    let out = run_baton_exchange("http://127.0.0.1:1", "this is not an envelope");

    assert!(
        !out.status.success(),
        "a malformed request must exit non-zero"
    );
    assert!(
        out.stdout.is_empty(),
        "malformed request writes nothing to stdout, got: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("request envelope"),
        "stderr diagnoses the malformed request: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// `SubprocessParticipant` driving the real compiled `baton exchange` binary.
//
// The unit tests in `src/participant.rs` drive the impl against `sh -c` stubs.
// This test adds confidence that the real spawn / stdin-write / stdout-read
// plumbing correlates a response envelope end-to-end, using the same
// in-process mock server the other exchange tests use. Credentials/base_url are
// passed as env overrides (API-key precedence pins the mock), so the spawned
// child talks only to the mock.
#[test]
fn subprocess_participant_round_trips_via_real_binary() {
    use baton::message::{MessageEnvelope, MessageKind};
    use baton::participant::{Participant, SubprocessParticipant};

    let server = MockServer::spawn(200, SUCCESS_BODY);
    let participant = SubprocessParticipant::new(
        env!("CARGO_BIN_EXE_baton"),
        ["exchange"],
        [
            ("ANTHROPIC_API_KEY", "test-key"),
            ("ANTHROPIC_BASE_URL", server.base_url()),
            ("BATON_MODEL", "claude-test-model"),
            ("BATON_TIMEOUT_SECS", "5"),
        ],
        Duration::from_secs(10),
    );

    let request = MessageEnvelope::new(
        "m-1",
        "conv-1",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "hi",
        1_700_000_000_000,
    );
    let response = participant.respond(&request);

    assert_eq!(response.kind, MessageKind::Response);
    assert_eq!(response.conversation_id, "conv-1");
    assert_eq!(response.in_reply_to.as_deref(), Some("m-1"));
    // Addressing swaps, and the body is the mock's reply.
    assert_eq!(response.from, "agent-b");
    assert_eq!(response.to, "agent-a");
    assert_eq!(response.body, "hello from the mock server");
    assert_ne!(response.message_id, "m-1");
    // The child's provider call rides along in-band.
    assert!(
        response.exchange.is_some(),
        "child nests its provider call record"
    );
}

// ---------------------------------------------------------------------------
// Vertical proof: `baton::converse` driving two *independent OS processes*.
//
// The M3c headline. The driver is handed two `SubprocessParticipant`s, each of
// which spawns a real `baton exchange` child per turn. The two children are
// pointed at two separate loopback mock servers (`127.0.0.1`, dummy API key),
// so a bounded conversation runs to a terminal condition with no external
// network and no in-process trait double — two genuinely independent agents
// driven over the envelope boundary.
#[test]
fn converse_drives_two_independent_processes_to_turn_cap() {
    use baton::converse::{Governance, TerminalReason, converse};
    use baton::message::{MessageEnvelope, MessageKind};
    use baton::participant::SubprocessParticipant;

    // One mock per side; each child talks only to its own mock.
    let server_a = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let server_b = MockServer::spawn_repeating(200, SUCCESS_BODY);

    let make = |base_url: &str, model: &'static str| {
        SubprocessParticipant::new(
            env!("CARGO_BIN_EXE_baton"),
            ["exchange"],
            [
                ("ANTHROPIC_API_KEY", "test-key"),
                ("ANTHROPIC_BASE_URL", base_url),
                ("BATON_MODEL", model),
                ("BATON_TIMEOUT_SECS", "5"),
            ],
            Duration::from_secs(10),
        )
    };
    let participant_a = make(server_a.base_url(), "model-a");
    let participant_b = make(server_b.base_url(), "model-b");

    let seed = MessageEnvelope::new(
        "conv-1-m0",
        "conv-1",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "kick off",
        1_700_000_000_000,
    );

    // The mock always returns 200, so neither child ever emits done/error; only
    // the turn-cap can stop the run — the termination guarantee proven across
    // real process boundaries.
    let governance = Governance {
        max_turns: 3,
        token_budget: None,
    };
    let transcript = converse(&participant_a, &participant_b, seed, &governance);

    assert_eq!(transcript.reason, TerminalReason::TurnCap);
    // Seed + exactly 3 replies.
    assert_eq!(transcript.trail.len(), 4);

    // Per-turn addressing coherence pinned end-to-end: each reply's `from` names
    // its actual speaker, alternating B, A, B (a double swap would mislabel it).
    assert_eq!(transcript.trail[0].from, "agent-a"); // the seed: A asks B
    assert_eq!(transcript.trail[0].to, "agent-b");
    assert_eq!(transcript.trail[1].from, "agent-b");
    assert_eq!(transcript.trail[1].to, "agent-a");
    assert_eq!(transcript.trail[2].from, "agent-a");
    assert_eq!(transcript.trail[2].to, "agent-b");
    assert_eq!(transcript.trail[3].from, "agent-b");
    assert_eq!(transcript.trail[3].to, "agent-a");

    // Every reply is a well-formed response carrying its child's provider call
    // in-band, and links to the request it answered.
    for reply in &transcript.trail[1..] {
        assert_eq!(reply.kind, MessageKind::Response);
        assert!(
            reply.exchange.is_some(),
            "each reply nests its child's call"
        );
        assert!(reply.in_reply_to.is_some(), "each reply links its request");
        assert_eq!(reply.conversation_id, "conv-1");
    }
}

// ---------------------------------------------------------------------------
// `baton converse` end-to-end.
//
// The driver logic is unit-tested in `src/converse.rs` and the two-process
// proof above; this test drives the compiled binary itself — it parses the
// `converse` command, builds two in-process participants from the environment,
// runs the governed loop against a repeating loopback mock, and writes the
// JSONL trail to stdout, ending on the turn-cap.
// ---------------------------------------------------------------------------
#[test]
fn converse_command_writes_jsonl_trail_and_ends_on_turn_cap() {
    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_baton"));
    cmd.args(["converse", "--seed", "kick off"]);
    cmd.env("ANTHROPIC_API_KEY", "test-key");
    cmd.env("ANTHROPIC_BASE_URL", server.base_url());
    cmd.env("BATON_MODEL", "claude-test-model");
    cmd.env("BATON_TIMEOUT_SECS", "5");
    cmd.env("BATON_MAX_TURNS", "2");
    cmd.env_remove("ANTHROPIC_AUTH_TOKEN");
    cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    cmd.env_remove("BATON_TOKEN_BUDGET");
    cmd.env_remove("BATON_EVENT_LOG");
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let out = cmd.output().expect("spawn baton converse");
    assert!(
        out.status.success(),
        "converse should exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    // Seed + 2 replies (BATON_MAX_TURNS=2).
    assert_eq!(lines.len(), 3, "seed + 2 reply turns: {stdout}");

    let seed: serde_json::Value = serde_json::from_str(lines[0]).expect("seed is JSON");
    assert_eq!(seed["kind"], "request");
    assert_eq!(seed["from"], "agent-a");
    assert_eq!(seed["to"], "agent-b");
    assert_eq!(seed["body"], "kick off");

    // Replies alternate speaker B, A and carry the mock's reply body in-band.
    let reply1: serde_json::Value = serde_json::from_str(lines[1]).expect("reply is JSON");
    assert_eq!(reply1["kind"], "response");
    assert_eq!(reply1["from"], "agent-b");
    assert_eq!(reply1["to"], "agent-a");
    assert_eq!(reply1["body"], "hello from the mock server");
    assert_eq!(reply1["exchange"]["schema"], "baton.exchange/v1");

    let reply2: serde_json::Value = serde_json::from_str(lines[2]).expect("reply is JSON");
    assert_eq!(reply2["from"], "agent-a");
    assert_eq!(reply2["to"], "agent-b");

    // The terminal reason is reported on stderr.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("TurnCap"),
        "stderr names the terminal reason: {stderr}"
    );
}

/// A truncated trailing line (no terminating newline — what a killed
/// `baton ask`/`session` leaves behind) does not brick `baton log show`: every
/// complete exchange before it is rendered, exit is 0, and a stderr warning
/// names the skipped partial line.
#[test]
fn log_show_tolerates_trailing_partial_line() {
    let temp = TempEventLog::new("show-partial");
    let trail = concat!(
        r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"first exchange"}"#,
        "\n",
        r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"first reply"}"#,
        "\n",
        r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000001000,"model":"claude-sonnet-4-6","base_url":"https://api.anthropic.com","prompt":"second exchange"}"#,
        "\n",
        r#"{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000001420,"duration_ms":418,"reply":"second reply"}"#,
        "\n",
        // Truncated trailing `request` with no terminating newline — an unclean
        // shutdown artefact. Without tolerance this hard-errors the whole file.
        r#"{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000002000,"model":"m","base_url":"u","prom"#,
    );
    std::fs::write(&temp.file, trail).expect("write trail");

    let out = run_baton_log(&["show", "--file", temp.file.to_str().unwrap()], None);
    assert!(
        out.status.success(),
        "show should succeed despite the trailing partial; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("first exchange"),
        "first exchange rendered: {stdout}"
    );
    assert!(
        stdout.contains("first reply"),
        "first reply rendered: {stdout}"
    );
    assert!(
        stdout.contains("second exchange"),
        "second exchange rendered: {stdout}"
    );
    assert!(
        stdout.contains("second reply"),
        "second reply rendered: {stdout}"
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("warning") && stderr.contains("line 5"),
        "stderr warns about the skipped partial line: {stderr}"
    );
}

/// `baton log replay` also tolerates a truncated trailing line and replays the
/// complete exchange that precedes it.
#[test]
fn log_replay_tolerates_trailing_partial_line() {
    let server = MockServer::spawn(200, SUCCESS_BODY);
    let source = TempEventLog::new("replay-partial-src");
    let sink = TempEventLog::new("replay-partial-sink");

    // The recorded request points at the mock server, so replay re-sends there.
    let trail = format!(
        concat!(
            r#"{{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000000000,"model":"claude-test-model","base_url":"{base}","prompt":"replay me"}}"#,
            "\n",
            r#"{{"event":"response_ok","schema":"baton.exchange/v1","ts_ms":1700000000420,"duration_ms":418,"reply":"old reply"}}"#,
            "\n",
            // Truncated trailing line with no terminating newline.
            r#"{{"event":"request","schema":"baton.exchange/v1","ts_ms":1700000001000,"trunc"#,
        ),
        base = server.base_url(),
    );
    std::fs::write(&source.file, trail).expect("write source trail");

    let out = run_baton_log(
        &[
            "replay",
            "--index",
            "1",
            "--file",
            source.file.to_str().unwrap(),
        ],
        Some(&sink.file),
    );
    assert!(
        out.status.success(),
        "replay should succeed despite the trailing partial; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // stdout is the fresh reply and nothing else — same contract as `ask`.
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "hello from the mock server"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("warning") && stderr.contains("line 3"),
        "stderr warns about the skipped partial line: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Async `baton converse` over a mailbox.
//
// The C1 headline: `baton converse --b-mailbox` drives a governed multi-turn
// conversation whose side B is a *live, independent* `baton serve` daemon,
// reached over the file-mailbox rather than in-process. A single repeating
// loopback mock stands in for the provider both sides call (it is
// content-agnostic, so one mock serves the converse process's side A and the
// serve process's side B). No external network; two genuinely independent
// processes coordinating only through `pending/` + the outbox.
// ---------------------------------------------------------------------------

/// A unique self-cleaning temp directory for a mailbox root, keyed by pid + tag.
struct TempMailbox {
    path: PathBuf,
}

impl TempMailbox {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("baton-cvmb-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp mailbox dir");
        Self { path }
    }
}

impl Drop for TempMailbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn converse_b_mailbox_drives_multi_turn_against_live_serve() {
    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let root = TempMailbox::new("async");
    let inbox = root.path.join("inbox");
    let outbox = root.path.join("outbox");

    // Side B: a live `serve` daemon consuming `inbox`, replying into `outbox`,
    // its provider calls answered by the mock. A tight poll keeps the driven
    // turns responsive.
    let mut serve = Command::new(env!("CARGO_BIN_EXE_baton"));
    serve.args([
        "serve",
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--poll-ms",
        "20",
    ]);
    serve.env("ANTHROPIC_API_KEY", "test-key");
    serve.env("ANTHROPIC_BASE_URL", server.base_url());
    serve.env("BATON_MODEL", "model-b");
    serve.env("BATON_TIMEOUT_SECS", "5");
    serve.env_remove("ANTHROPIC_AUTH_TOKEN");
    serve.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    serve.env_remove("BATON_EVENT_LOG");
    serve.stdout(Stdio::null());
    serve.stderr(Stdio::null());
    let mut serve_child = serve.spawn().expect("spawn baton serve");

    // Side A: the in-process participant inside the `converse` process. Delivery
    // to `pending/` does not require the daemon to be up yet — the generous
    // await covers any startup lag — so no explicit readiness handshake.
    let mut converse = Command::new(env!("CARGO_BIN_EXE_baton"));
    converse.args([
        "converse",
        "--seed",
        "kick off",
        "--a-model",
        "model-a",
        "--b-mailbox",
        "--b-inbox",
        inbox.to_str().unwrap(),
        "--b-outbox",
        outbox.to_str().unwrap(),
        "--b-await-ms",
        "10000",
    ]);
    converse.env("ANTHROPIC_API_KEY", "test-key");
    converse.env("ANTHROPIC_BASE_URL", server.base_url());
    converse.env("BATON_MODEL", "claude-test-model");
    converse.env("BATON_TIMEOUT_SECS", "5");
    converse.env("BATON_MAX_TURNS", "2");
    converse.env_remove("ANTHROPIC_AUTH_TOKEN");
    converse.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    converse.env_remove("BATON_TOKEN_BUDGET");
    converse.env_remove("BATON_EVENT_LOG");
    converse.stdout(Stdio::piped());
    converse.stderr(Stdio::piped());

    let out = converse.output().expect("run baton converse");

    // Tear the daemon down cooperatively regardless of the assertions below.
    let _ = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["serve", "--stop", "--inbox", inbox.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = serve_child.wait();

    assert!(
        out.status.success(),
        "converse should exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    // Seed + 2 replies (BATON_MAX_TURNS=2).
    assert_eq!(lines.len(), 3, "seed + 2 reply turns: {stdout}");

    let seed: serde_json::Value = serde_json::from_str(lines[0]).expect("seed is JSON");
    assert_eq!(seed["kind"], "request");
    assert_eq!(seed["from"], "agent-a");
    assert_eq!(seed["to"], "agent-b");

    // Turn 1 is B, answered over the mailbox by the live `serve` peer: a
    // `response` carrying the peer's provider call in-band and correlated to the
    // seed. This is the async round-trip that proves the mailbox-backed
    // participant.
    let reply_b: serde_json::Value = serde_json::from_str(lines[1]).expect("B reply is JSON");
    assert_eq!(reply_b["kind"], "response");
    assert_eq!(reply_b["from"], "agent-b");
    assert_eq!(reply_b["to"], "agent-a");
    assert_eq!(reply_b["body"], "hello from the mock server");
    assert_eq!(
        reply_b["exchange"]["schema"], "baton.exchange/v1",
        "the served peer nests its provider call in-band"
    );
    assert!(
        reply_b["in_reply_to"].is_string(),
        "B's reply links its request"
    );

    // Turn 2 is A (in-process), completing the alternation.
    let reply_a: serde_json::Value = serde_json::from_str(lines[2]).expect("A reply is JSON");
    assert_eq!(reply_a["kind"], "response");
    assert_eq!(reply_a["from"], "agent-a");
    assert_eq!(reply_a["to"], "agent-b");

    // Governance still bounds the driven conversation exactly as in-process.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("TurnCap"),
        "stderr names the terminal reason: {stderr}"
    );
}

/// When no `serve` peer ever answers, the mailbox-backed B synthesizes a
/// transport-timeout terminal: the driver stops waiting after `--b-await-ms`
/// and records a `kind:"error"` turn with **no** nested record — distinct in
/// the trail from a peer-delivered error (which nests the peer's call).
#[test]
fn converse_b_mailbox_times_out_when_no_peer_answers() {
    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let root = TempMailbox::new("timeout");
    let inbox = root.path.join("inbox");
    let outbox = root.path.join("outbox");
    // No `serve` daemon is started, so no reply is ever delivered.

    let mut converse = Command::new(env!("CARGO_BIN_EXE_baton"));
    converse.args([
        "converse",
        "--seed",
        "kick off",
        "--a-model",
        "model-a",
        "--b-mailbox",
        "--b-inbox",
        inbox.to_str().unwrap(),
        "--b-outbox",
        outbox.to_str().unwrap(),
        "--b-await-ms",
        "300",
    ]);
    converse.env("ANTHROPIC_API_KEY", "test-key");
    converse.env("ANTHROPIC_BASE_URL", server.base_url());
    converse.env("BATON_MODEL", "claude-test-model");
    converse.env("BATON_TIMEOUT_SECS", "5");
    converse.env("BATON_MAX_TURNS", "4");
    converse.env_remove("ANTHROPIC_AUTH_TOKEN");
    converse.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    converse.env_remove("BATON_TOKEN_BUDGET");
    converse.env_remove("BATON_EVENT_LOG");
    converse.stdout(Stdio::piped());
    converse.stderr(Stdio::piped());

    let out = converse.output().expect("run baton converse");
    assert!(
        out.status.success(),
        "converse exits 0 even when B times out; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    // Seed + B's synthesized timeout turn — the run ends on the error, well
    // before the turn-cap of 4.
    assert_eq!(lines.len(), 2, "seed + one terminal error turn: {stdout}");

    let reply_b: serde_json::Value = serde_json::from_str(lines[1]).expect("B turn is JSON");
    assert_eq!(reply_b["kind"], "error");
    assert!(
        reply_b["exchange"].is_null(),
        "a driver-timeout nests no provider record (unlike a peer-delivered error)"
    );
    assert!(
        reply_b["body"].as_str().unwrap().contains("timed out"),
        "the timeout turn names the await-timeout: {}",
        reply_b["body"]
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Error"),
        "stderr names the terminal reason: {stderr}"
    );
}

/// The operator quickstart (`scripts/quickstart.sh`) runs the full A2A loop
/// against the loopback mock — no network, no credential — and exits 0 having
/// written both trails. This keeps the shipped demo artifact CI-covered.
///
/// The mock lives under `examples/`, for which cargo exposes no
/// `CARGO_BIN_EXE_*`; the test builds it explicitly and derives its path from
/// the `baton` bin's directory, so the run never depends on cargo's example
/// build-ordering.
#[test]
fn quickstart_script_runs_full_loop_against_mock() {
    // Build the mock example explicitly (idempotent / cached) so its compiled
    // path is guaranteed present before the script runs.
    let cargo = option_env!("CARGO").unwrap_or("cargo");
    let built = Command::new(cargo)
        .args(["build", "--example", "mock_provider"])
        .status()
        .expect("build mock_provider example");
    assert!(built.success(), "mock_provider example builds");

    let baton_bin = PathBuf::from(env!("CARGO_BIN_EXE_baton"));
    // `<target>/<profile>/baton` -> `<target>/<profile>/examples/mock_provider`.
    let mock_bin = baton_bin
        .parent()
        .expect("baton bin has a parent dir")
        .join("examples")
        .join("mock_provider");
    assert!(mock_bin.exists(), "mock_provider at {}", mock_bin.display());

    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("quickstart.sh");

    let out_dir = std::env::temp_dir().join(format!("baton-quickstart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);

    // The script configures its own provider env; strip any host leakage so the
    // run is deterministic regardless of the developer's shell.
    let out = Command::new("bash")
        .arg(&script)
        .env("BATON_BIN", &baton_bin)
        .env("BATON_MOCK_BIN", &mock_bin)
        .env("QUICKSTART_OUT", &out_dir)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .env_remove("ANTHROPIC_BASE_URL")
        .env_remove("BATON_EVENT_LOG")
        .output()
        .expect("run quickstart.sh");

    assert!(
        out.status.success(),
        "quickstart.sh exits 0; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // Both trails exist, are non-empty, and the printed paths name them.
    let converse_trail = out_dir.join("converse-trail.jsonl");
    let reply_trail = out_dir.join("serve-send-reply.jsonl");
    for trail in [&converse_trail, &reply_trail] {
        let bytes =
            std::fs::read(trail).unwrap_or_else(|e| panic!("read {}: {e}", trail.display()));
        assert!(!bytes.is_empty(), "{} is non-empty", trail.display());
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains(converse_trail.to_str().unwrap()),
        "stdout names the converse trail path: {stdout}"
    );
    assert!(
        stdout.contains(reply_trail.to_str().unwrap()),
        "stdout names the serve+send reply path: {stdout}"
    );

    // The consumed reply is a well-formed, correlated response envelope.
    let reply_line = std::fs::read_to_string(&reply_trail).expect("read reply trail");
    let reply: serde_json::Value =
        serde_json::from_str(reply_line.trim()).expect("reply is one JSON line");
    assert_eq!(reply["kind"], "response");
    assert!(
        reply["in_reply_to"].is_string(),
        "the consumed reply correlates to the sent request"
    );

    let _ = std::fs::remove_dir_all(&out_dir);
}
