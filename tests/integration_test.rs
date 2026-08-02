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
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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

/// Reads a complete HTTP request before the mock writes a response. Closing a
/// socket with unread request body bytes can cause Windows to reset the
/// connection instead of delivering that response.
fn drain_request(stream: &mut TcpStream) -> Vec<u8> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];

    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return request,
            Ok(read) => request.extend_from_slice(&chunk[..read]),
        }
    }

    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("request headers were read")
        + 4;
    let content_length = std::str::from_utf8(&request[..header_end])
        .ok()
        .and_then(|headers| {
            headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        });

    let Some(content_length) = content_length else {
        return request;
    };
    let body_end = header_end + content_length;
    while request.len() < body_end {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => request.extend_from_slice(&chunk[..read]),
        }
    }
    request
}

#[test]
fn global_help_and_version_flags_succeed_without_configuration() {
    for (flag, expected) in [
        ("--help", "Global options:"),
        ("-h", "Global options:"),
        ("--version", env!("CARGO_PKG_VERSION")),
        ("-V", env!("CARGO_PKG_VERSION")),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_baton"))
            .arg(flag)
            .env_remove("ANTHROPIC_API_KEY")
            .env_remove("ANTHROPIC_AUTH_TOKEN")
            .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
            .output()
            .expect("run baton global flag");
        assert!(
            out.status.success(),
            "{flag} should succeed; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.stderr.is_empty(), "{flag} stderr must be empty");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains(expected), "{flag} stdout: {stdout}");
    }
}

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
                // Drain the request before replying. One request/response per
                // connection.
                let _ = drain_request(&mut stream);

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
                    let _ = drain_request(&mut stream);
                    thread::sleep(Duration::from_secs(30));
                    return;
                }

                // Drain the request. We don't care about its contents for the
                // status-mapping tests, but we must read it so the client's
                // `send` returns; otherwise the OS buffer fills and the
                // server-side write blocks.
                let _ = drain_request(&mut stream);

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
            let buf = drain_request(&mut stream);
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
            let buf = drain_request(&mut stream);
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
    assert!(
        response.get("session_id").is_none() && response.get("turn_index").is_none(),
        "ask outcomes remain sessionless: {response:?}"
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

    // The session is self-delimiting: a session_start marker, then two turns ×
    // (request + response_ok), then a session_end marker reporting the turn count.
    let lines = read_jsonl(&temp.file);
    assert_eq!(
        lines.len(),
        6,
        "session_start + two turns × (request + response_ok) + session_end, got {lines:?}"
    );
    assert_eq!(lines[0]["event"], "session_start");
    let session_id = lines[0]["session_id"]
        .as_str()
        .expect("session_start carries a session_id")
        .to_string();

    assert_eq!(lines[1]["event"], "request");
    assert_eq!(lines[1]["prompt"], "first turn");
    assert_eq!(lines[1]["session_id"], session_id);
    assert_eq!(lines[1]["turn_index"], 0);
    assert_eq!(lines[2]["event"], "response_ok");
    assert_eq!(lines[2]["session_id"], session_id);
    assert_eq!(lines[2]["turn_index"], 0);

    assert_eq!(lines[3]["event"], "request");
    assert_eq!(lines[3]["prompt"], "second turn");
    assert_eq!(lines[3]["session_id"], session_id);
    assert_eq!(lines[3]["turn_index"], 1);
    assert_eq!(lines[4]["event"], "response_ok");
    assert_eq!(lines[4]["session_id"], session_id);
    assert_eq!(lines[4]["turn_index"], 1);

    assert_eq!(lines[5]["event"], "session_end");
    assert_eq!(lines[5]["session_id"], session_id);
    assert_eq!(lines[5]["turns"], 2);

    // Every turn's request carries the one session_id stamped by session_start —
    // the key that partitions a shared trail back into whole sessions.
    let turn_ids: Vec<&str> = lines
        .iter()
        .filter(|l| l["event"] == "request")
        .map(|l| l["session_id"].as_str().expect("session turn carries id"))
        .collect();
    assert_eq!(turn_ids, vec![session_id.as_str(), session_id.as_str()]);
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
fn roleless_external_agent_serve_does_not_require_home() {
    let root = TempMailbox::new("no-role-home");
    let inbox = root.path.join("inbox");
    let outbox = root.path.join("outbox");

    let mut serve = Command::new(env!("CARGO_BIN_EXE_baton"));
    serve.args([
        "serve",
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--once",
        "--agent-cmd",
        "baton-test-agent-stub",
    ]);
    // This is the regression boundary: a role-less external agent does not
    // need any Baton home or provider environment, even on an empty mailbox.
    serve.env_clear();

    let out = serve.output().expect("run role-less external-agent serve");
    assert!(
        out.status.success(),
        "serve should succeed without a home; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    for directory in [
        inbox.join("pending"),
        inbox.join("claimed"),
        inbox.join("done"),
    ] {
        assert!(
            directory.is_dir(),
            "serve should create mailbox directory {}",
            directory.display()
        );
    }
}

#[cfg(unix)]
#[test]
fn external_agent_serve_forwards_raw_args_and_mailbox_body() {
    let root = TempMailbox::new("external-agent-args");
    let inbox = root.path.join("inbox");
    let outbox = root.path.join("outbox");
    let stub = root.path.join("agent-stub");
    let captured_args = root.path.join("agent-args.txt");
    let captured_stdin = root.path.join("agent-stdin.txt");

    // The stub is the actual `--agent-cmd` executable. Its positional arguments
    // are recorded before it returns a deterministic free-text response.
    std::fs::write(
        &stub,
        r#"#!/bin/sh
set -eu
cat > "$BATON_TEST_STDIN"
printf '%s\n' "$@" > "$BATON_TEST_ARGS"
printf 'stub response'
"#,
    )
    .expect("write external-agent stub");
    let mut permissions = std::fs::metadata(&stub)
        .expect("stat external-agent stub")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&stub, permissions).expect("make external-agent stub executable");

    let expected_args = [
        "--append-system-prompt",
        "caller-owned identity",
        "--dangerously-skip-permissions",
        "--mcp-config=/tmp/caller-mcp.json",
    ];
    let request_body = "request reaches the external agent";

    let mut serve = Command::new(env!("CARGO_BIN_EXE_baton"));
    serve.args([
        "serve",
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--poll-ms",
        "10",
        "--agent-cmd",
        stub.to_str().unwrap(),
        "--agent-arg",
        expected_args[0],
        "--agent-arg",
        expected_args[1],
        "--agent-arg",
        expected_args[2],
        "--agent-arg",
        expected_args[3],
    ]);
    // External-agent mode must not depend on Baton provider configuration or a
    // host event-log setting; only the stub's capture paths are inherited.
    serve
        .env_clear()
        .env("BATON_TEST_ARGS", &captured_args)
        .env("BATON_TEST_STDIN", &captured_stdin)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let serve_child = serve.spawn().expect("spawn external-agent serve");

    let mut send = Command::new(env!("CARGO_BIN_EXE_baton"));
    send.args([
        "send",
        "--inbox",
        inbox.to_str().unwrap(),
        "--outbox",
        outbox.to_str().unwrap(),
        "--await",
        "--timeout-ms",
        "10000",
        "--to",
        "worker",
        "--body",
        request_body,
    ]);
    send.env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let send_output = send.output().expect("send mailbox request");

    // Stop and reap the daemon before reading captures or asserting, so a
    // failed assertion cannot leave a live serve process behind.
    let stop = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["serve", "--stop", "--inbox", inbox.to_str().unwrap()])
        .env_clear()
        .output()
        .expect("stop external-agent serve");
    let serve_output = serve_child
        .wait_with_output()
        .expect("reap external-agent serve");

    assert!(
        stop.status.success(),
        "serve stop should succeed; stderr: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    assert!(
        serve_output.status.success(),
        "serve should exit 0; stderr: {}",
        String::from_utf8_lossy(&serve_output.stderr)
    );
    assert!(
        send_output.status.success(),
        "send should receive a response; stderr: {}",
        String::from_utf8_lossy(&send_output.stderr)
    );

    let args = std::fs::read_to_string(&captured_args).expect("read captured agent args");
    assert_eq!(args, format!("{}\n", expected_args.join("\n")));
    assert_eq!(
        std::fs::read_to_string(&captured_stdin).expect("read captured agent stdin"),
        request_body
    );

    let response: serde_json::Value =
        serde_json::from_slice(&send_output.stdout).expect("awaited response is JSON");
    assert_eq!(response["kind"], "response");
    assert_eq!(response["body"], "stub response");
    assert!(
        response["in_reply_to"].is_string(),
        "mailbox response correlates to the request"
    );
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
///
/// Unix-only: `quickstart.sh` is a bash artifact and the mock binary carries no
/// `.exe` suffix, so the harness assumptions hold on Unix (Linux + macOS) only.
#[cfg(unix)]
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

// ---------------------------------------------------------------------------
// N-party `baton converse-ring` over a static routing registry.
//
// The registry maps each participant name to its `{inbox, outbox}` pair; the
// ring driver resolves every roster name at startup and builds one live,
// mailbox-backed peer per member. Here three independent `baton serve` daemons
// (alice / bob / carol) answer over their own mailboxes, all provider calls met
// by one content-agnostic repeating mock. This is the end-to-end proof that the
// registry wires an N-party round-robin conversation.
// ---------------------------------------------------------------------------

/// Spawns a `baton serve` daemon for one ring member, consuming `<root>/inbox`
/// and replying into `<root>/outbox`, its provider calls answered by `base_url`.
fn spawn_ring_serve(member_root: &Path, base_url: &str, model: &str) -> std::process::Child {
    let mut serve = Command::new(env!("CARGO_BIN_EXE_baton"));
    serve.args([
        "serve",
        "--inbox",
        member_root.join("inbox").to_str().unwrap(),
        "--outbox",
        member_root.join("outbox").to_str().unwrap(),
        "--poll-ms",
        "20",
    ]);
    serve.env("ANTHROPIC_API_KEY", "test-key");
    serve.env("ANTHROPIC_BASE_URL", base_url);
    serve.env("BATON_MODEL", model);
    serve.env("BATON_TIMEOUT_SECS", "5");
    serve.env_remove("ANTHROPIC_AUTH_TOKEN");
    serve.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    serve.env_remove("BATON_EVENT_LOG");
    serve.stdout(Stdio::null());
    serve.stderr(Stdio::null());
    serve.spawn().expect("spawn baton serve")
}

/// Cooperatively stops the `serve` daemon consuming `<root>/inbox`.
fn stop_ring_serve(member_root: &Path) {
    let _ = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "serve",
            "--stop",
            "--inbox",
            member_root.join("inbox").to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[test]
fn converse_ring_drives_three_live_serve_peers() {
    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let root = TempMailbox::new("ring");
    let alice = root.path.join("alice");
    let bob = root.path.join("bob");
    let carol = root.path.join("carol");

    // A registry mapping each roster name to its own mailbox pair (a pair, not a
    // single path). Absolute paths so the driver resolves them regardless of cwd.
    let registry_json = serde_json::json!({
        "participants": {
            "alice": {
                "inbox": alice.join("inbox").to_string_lossy(),
                "outbox": alice.join("outbox").to_string_lossy(),
            },
            "bob": {
                "inbox": bob.join("inbox").to_string_lossy(),
                "outbox": bob.join("outbox").to_string_lossy(),
            },
            "carol": {
                "inbox": carol.join("inbox").to_string_lossy(),
                "outbox": carol.join("outbox").to_string_lossy(),
            },
        },
    });
    let registry_path = root.path.join("registry.json");
    std::fs::write(
        &registry_path,
        serde_json::to_string(&registry_json).expect("serialize registry"),
    )
    .expect("write registry");

    // Three independent peers; each is a full `baton serve` daemon.
    let mut alice_child = spawn_ring_serve(&alice, server.base_url(), "model-alice");
    let mut bob_child = spawn_ring_serve(&bob, server.base_url(), "model-bob");
    let mut carol_child = spawn_ring_serve(&carol, server.base_url(), "model-carol");

    let mut ring = Command::new(env!("CARGO_BIN_EXE_baton"));
    ring.args([
        "converse-ring",
        "--registry",
        registry_path.to_str().unwrap(),
        "--roster",
        "alice,bob,carol",
        "--seed",
        "kick off",
        "--await-ms",
        "10000",
    ]);
    // The driver itself runs no provider call, but `Governance::from_lookup`
    // reads the turn-cap; three turns visit bob, carol, then alice (the wrap).
    ring.env("BATON_MAX_TURNS", "3");
    ring.env_remove("BATON_TOKEN_BUDGET");
    ring.env_remove("BATON_EVENT_LOG");
    ring.stdout(Stdio::piped());
    ring.stderr(Stdio::piped());

    let out = ring.output().expect("run baton converse-ring");

    // Tear all three peers down regardless of the assertions below.
    stop_ring_serve(&alice);
    stop_ring_serve(&bob);
    stop_ring_serve(&carol);
    let _ = alice_child.wait();
    let _ = bob_child.wait();
    let _ = carol_child.wait();

    assert!(
        out.status.success(),
        "converse-ring should exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    // Seed + 3 reply turns (BATON_MAX_TURNS=3).
    assert_eq!(lines.len(), 4, "seed + 3 reply turns: {stdout}");

    let seed: serde_json::Value = serde_json::from_str(lines[0]).expect("seed is JSON");
    assert_eq!(seed["kind"], "request");
    assert_eq!(seed["from"], "alice", "seed is addressed from roster[0]");
    assert_eq!(seed["to"], "bob", "seed is addressed to roster[1]");

    // Round-robin order: each reply's authoritative speaker (`from`) advances by
    // ring position — bob, carol, then alice on the wrap.
    let speakers: Vec<String> = lines[1..]
        .iter()
        .map(|line| {
            let reply: serde_json::Value = serde_json::from_str(line).expect("reply is JSON");
            assert_eq!(reply["kind"], "response", "each peer answers: {line}");
            assert_eq!(
                reply["exchange"]["schema"], "baton.exchange/v1",
                "each served peer nests its provider call in-band"
            );
            reply["from"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        speakers,
        vec!["bob".to_string(), "carol".to_string(), "alice".to_string()],
        "round-robin visits the ring in order, wrapping past roster[0]"
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("TurnCap"),
        "stderr names the terminal reason: {stderr}"
    );
}

#[test]
fn converse_ring_unknown_roster_name_is_startup_error() {
    // A roster name absent from the registry must fail fast at startup — before
    // any turn runs and without needing a live peer.
    let root = TempMailbox::new("ring-unknown");
    let registry_json = serde_json::json!({
        "participants": {
            "alice": {
                "inbox": root.path.join("alice/inbox").to_string_lossy(),
                "outbox": root.path.join("alice/outbox").to_string_lossy(),
            },
            "bob": {
                "inbox": root.path.join("bob/inbox").to_string_lossy(),
                "outbox": root.path.join("bob/outbox").to_string_lossy(),
            },
        },
    });
    let registry_path = root.path.join("registry.json");
    std::fs::write(
        &registry_path,
        serde_json::to_string(&registry_json).expect("serialize registry"),
    )
    .expect("write registry");

    let mut ring = Command::new(env!("CARGO_BIN_EXE_baton"));
    ring.args([
        "converse-ring",
        "--registry",
        registry_path.to_str().unwrap(),
        "--roster",
        "alice,bob,ghost",
        "--seed",
        "kick off",
    ]);
    ring.env_remove("BATON_EVENT_LOG");
    ring.stdout(Stdio::piped());
    ring.stderr(Stdio::piped());

    let out = ring.output().expect("run baton converse-ring");
    assert!(
        !out.status.success(),
        "an unknown roster name is a startup error (non-zero exit)"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ghost"),
        "the error names the unroutable participant: {stderr}"
    );
    assert!(
        out.stdout.is_empty(),
        "no trail is written when startup fails: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// Reads `/proc/<pid>/status`'s `PPid` field. Linux-only — this crate has no
/// portable process-parentage lookup, and this regression exists specifically
/// to prove parentage.
#[cfg(target_os = "linux")]
fn read_ppid(pid: u32) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok())
}

/// Reports whether a Linux process is still running rather than a zombie.
#[cfg(target_os = "linux")]
fn process_is_live(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some(after_comm) = stat.rsplit_once(')').map(|(_, rest)| rest) else {
        return false;
    };
    after_comm
        .split_whitespace()
        .next()
        .is_some_and(|state| state != "Z")
}

/// Issue #109 AC #5 (orphan-survival regression): a session started through
/// `baton service start` is spawned as a **direct child of the long-lived
/// `baton service run`**, not of the short-lived `service start` client that
/// submitted it — so it survives that client's exit (and could never have
/// been taken down by a kill of the client's process tree, since it was never
/// part of it). Linux-only: the parentage proof reads `/proc/<pid>/status`,
/// which has no portable equivalent in this crate.
#[cfg(target_os = "linux")]
#[test]
fn service_session_survives_submitting_client_and_is_owned_by_run() {
    use baton::mailbox;
    use baton::message::{MessageEnvelope, MessageKind};

    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let root = TempMailbox::new("service");
    let control = root.path.join("control");
    let inbox = root.path.join("inbox");
    let outbox = root.path.join("outbox");

    // The long-lived supervisor. The mock provider credentials/model live
    // here, not on the short-lived `service start` client below — the spawned
    // `serve` session inherits *this* process's environment, since `Run` is
    // its real parent.
    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.env("ANTHROPIC_API_KEY", "test-key");
    run.env("ANTHROPIC_BASE_URL", server.base_url());
    run.env("BATON_MODEL", "model-service");
    run.env("BATON_TIMEOUT_SECS", "5");
    run.env_remove("ANTHROPIC_AUTH_TOKEN");
    run.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    run.env_remove("BATON_EVENT_LOG");
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn baton service run");
    let run_pid = run_child.id();

    let control_str = control.to_str().unwrap();

    // Wait for `Run` to acquire its control lock: `Start` fails fast rather
    // than waiting when no live service is found yet.
    let mut live = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(live, "baton service run did not report live in time");

    // The short-lived submitting client: it starts the session and has fully
    // exited (`.output()` waits for it) by the time this call returns — there
    // is no lingering process tree behind it to kill.
    let start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            inbox.to_str().unwrap(),
            "--outbox",
            outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
        ])
        .output()
        .expect("run baton service start");
    assert!(
        start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let session_id = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!session_id.is_empty(), "service start prints a session id");

    // Structural proof: the session's real PID's parent is `service run`,
    // never the already-exited `service start` client.
    let status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "status",
            "--control",
            control_str,
            "--session",
            &session_id,
        ])
        .output()
        .expect("run baton service status");
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("status is JSON");
    let sessions = status_json["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 1, "status reports the started session");
    assert_eq!(sessions[0]["live"], true, "the session reads as live");
    let serve_pid = sessions[0]["pid"].as_u64().expect("pid") as u32;
    let ppid = read_ppid(serve_pid).expect("the serve session has a PPid");
    assert_eq!(
        ppid, run_pid,
        "the serve session's parent is the live `service run`, not the exited submitter"
    );

    // Functional proof: a message delivered after the submitter is long gone
    // is still consumed by the still-running, service-owned session.
    let request = MessageEnvelope::new(
        "svc-m1",
        "conv-svc",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "hello",
        1_700_000_000_000,
    );
    mailbox::deliver_to(&inbox, &request).expect("deliver to the live session's inbox");
    let mut reply = None;
    for _ in 0..100 {
        if let Ok(Some(envelope)) = mailbox::try_claim_response(&outbox, "svc-m1") {
            reply = Some(envelope);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let reply = reply.expect("the still-running session answers a message from a later sender");
    assert_eq!(reply.body, "hello from the mock server");

    // Teardown reaps the session and stops `Run` cooperatively; wait it out.
    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .output()
        .expect("run baton service teardown");
    assert!(
        teardown.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    let run_status = run_child.wait().expect("baton service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");
}

/// Issue #123 regression: task admission and session cleanup are serialized
/// by a separate lock. The stop path reaps an already-admitted task, rejects
/// a racing task request after owner removal, and teardown reaps another
/// running task whose callback inbox is outside the owning session.
#[cfg(target_os = "linux")]
#[test]
fn service_stop_serializes_task_admission_and_reaps_owned_tasks() {
    use baton::mailbox;
    use baton::message::{MessageEnvelope, MessageKind};

    let root = TempMailbox::new("task-cleanup-race");
    let control = root.path.join("control");
    let session_inbox = root.path.join("session-inbox");
    let session_outbox = root.path.join("session-outbox");
    let agent_started = root.path.join("agent-started");
    let callback_inbox = root.path.join("callback");
    let racing_marker = root.path.join("racing-marker");

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn baton service run");
    let control_str = control.to_str().unwrap();

    let mut live = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(live, "baton service run did not report live in time");

    let session_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            session_inbox.to_str().unwrap(),
            "--outbox",
            session_outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
            "--agent-cmd",
            "sh",
            "--agent-arg",
            "-c",
            "--agent-arg",
            "cat >/dev/null; touch \"$1\"; sleep 30",
            "--agent-arg",
            "task-stop-agent",
            "--agent-arg",
            agent_started.to_str().unwrap(),
        ])
        .output()
        .expect("run service start for cleanup race");
    assert!(
        session_start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&session_start.stderr)
    );
    let session_id = String::from_utf8_lossy(&session_start.stdout)
        .trim()
        .to_string();
    assert!(!session_id.is_empty(), "service start prints a session id");

    let admitted_task = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "start",
            "--control",
            control_str,
            "--session",
            &session_id,
            "--command",
            "sleep",
            "--arg",
            "30",
            "--max-duration-ms",
            "60000",
            "--callback-inbox",
            callback_inbox.to_str().unwrap(),
        ])
        .output()
        .expect("start task under live owner");
    assert!(
        admitted_task.status.success(),
        "task start should succeed for a live owner; stderr: {}",
        String::from_utf8_lossy(&admitted_task.stderr)
    );
    let admitted_task_id = String::from_utf8_lossy(&admitted_task.stdout)
        .trim()
        .to_string();
    assert!(!admitted_task_id.is_empty(), "task start prints a task id");
    let admitted_status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "status",
            "--control",
            control_str,
            "--task",
            &admitted_task_id,
        ])
        .output()
        .expect("read admitted task status");
    let admitted_json: serde_json::Value =
        serde_json::from_slice(&admitted_status.stdout).expect("admitted task status is JSON");
    let admitted_pid = admitted_json["tasks"][0]["pid"]
        .as_u64()
        .expect("admitted task pid") as u32;
    assert!(
        process_is_live(admitted_pid),
        "admitted task should be running"
    );

    let request = MessageEnvelope::new(
        "task-stop-race-m1",
        "conv-task-stop-race",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "hold this turn",
        1_700_000_000_000,
    );
    mailbox::deliver_to(&session_inbox, &request).expect("deliver in-flight session request");
    let mut in_flight = false;
    for _ in 0..100 {
        if agent_started.is_file() {
            in_flight = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(in_flight, "session did not enter its in-flight agent turn");

    let stop = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "stop",
            "--control",
            control_str,
            "--session",
            &session_id,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn service stop");
    let session_stop = session_inbox.join("serve.stop");
    let mut draining = false;
    for _ in 0..250 {
        if session_stop.is_file() {
            draining = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(draining, "service stop did not begin draining the session");

    let racing_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "start",
            "--control",
            control_str,
            "--session",
            &session_id,
            "--command",
            "sh",
            "--arg",
            "-c",
            "--arg",
            "touch \"$1\"; sleep 30",
            "--arg",
            "racing-task",
            "--arg",
            racing_marker.to_str().unwrap(),
            "--max-duration-ms",
            "60000",
            "--callback-inbox",
            callback_inbox.to_str().unwrap(),
        ])
        .output()
        .expect("run racing task start");
    let stop_output = stop.wait_with_output().expect("wait for service stop");
    assert!(
        stop_output.status.success(),
        "service stop should exit 0; stderr: {}",
        String::from_utf8_lossy(&stop_output.stderr)
    );
    assert!(
        !racing_start.status.success(),
        "task admission racing cleanup must be rejected; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&racing_start.stdout),
        String::from_utf8_lossy(&racing_start.stderr)
    );
    assert!(
        String::from_utf8_lossy(&racing_start.stderr)
            .contains("does not name a live managed session"),
        "racing rejection should explain the owner failure: {}",
        String::from_utf8_lossy(&racing_start.stderr)
    );
    assert!(
        !racing_marker.exists(),
        "rejected racing task must not spawn"
    );

    let mut admitted_reaped = false;
    for _ in 0..100 {
        let status = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["task", "status", "--control", control_str])
            .output()
            .expect("read task status after service stop");
        let json: serde_json::Value =
            serde_json::from_slice(&status.stdout).expect("task status is JSON");
        if json["tasks"].as_array().unwrap().is_empty() && !process_is_live(admitted_pid) {
            admitted_reaped = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        admitted_reaped,
        "service stop must remove the admitted task and stop its process"
    );

    let second_inbox = root.path.join("second-session-inbox");
    let second_outbox = root.path.join("second-session-outbox");
    let second_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            second_inbox.to_str().unwrap(),
            "--outbox",
            second_outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
            "--agent-cmd",
            "sh",
            "--agent-arg",
            "-c",
            "--agent-arg",
            "cat >/dev/null; printf ready",
        ])
        .output()
        .expect("run second service start");
    assert!(
        second_start.status.success(),
        "second service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&second_start.stderr)
    );
    let second_session = String::from_utf8_lossy(&second_start.stdout)
        .trim()
        .to_string();
    assert!(
        !second_session.is_empty(),
        "second service start prints a session id"
    );

    let teardown_task = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "start",
            "--control",
            control_str,
            "--session",
            &second_session,
            "--command",
            "sleep",
            "--arg",
            "30",
            "--max-duration-ms",
            "60000",
            "--callback-inbox",
            callback_inbox.to_str().unwrap(),
        ])
        .output()
        .expect("start task for teardown cleanup");
    assert!(
        teardown_task.status.success(),
        "task start under second live owner should succeed; stderr: {}",
        String::from_utf8_lossy(&teardown_task.stderr)
    );
    let teardown_task_id = String::from_utf8_lossy(&teardown_task.stdout)
        .trim()
        .to_string();
    let teardown_task_status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "status",
            "--control",
            control_str,
            "--task",
            &teardown_task_id,
        ])
        .output()
        .expect("read teardown task status");
    let teardown_task_json: serde_json::Value =
        serde_json::from_slice(&teardown_task_status.stdout).expect("teardown task status JSON");
    let teardown_task_pid = teardown_task_json["tasks"][0]["pid"]
        .as_u64()
        .expect("teardown task pid") as u32;

    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .output()
        .expect("run service teardown");
    assert!(
        teardown.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    let run_status = run_child.wait().expect("baton service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");

    let final_status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["task", "status", "--control", control_str])
        .output()
        .expect("read task status after teardown");
    let final_json: serde_json::Value =
        serde_json::from_slice(&final_status.stdout).expect("final task status is JSON");
    assert!(
        final_json["tasks"].as_array().unwrap().is_empty(),
        "teardown removes the running task record"
    );
    assert!(
        !process_is_live(teardown_task_pid),
        "teardown stops the running task process"
    );
}

/// Issue #119 regression: `service start` resolves relative session paths in
/// the submitting client's working directory before the independent
/// `service run` process reconstructs the child argv. The test also keeps the
/// supervisor and teardown clients in different directories, and bounds
/// teardown below the cooperative-stop grace so a wrong relative inbox would
/// fail rather than passing after process-group escalation.
#[cfg(target_os = "linux")]
#[test]
fn service_start_resolves_relative_paths_from_submitting_client() {
    use baton::mailbox;
    use baton::message::{MessageEnvelope, MessageKind};

    let server = MockServer::spawn_repeating(200, SUCCESS_BODY);
    let root = TempMailbox::new("service-relative");
    let supervisor_dir = root.path.join("supervisor");
    let client_dir = root.path.join("client");
    std::fs::create_dir_all(&supervisor_dir).expect("create supervisor directory");
    std::fs::create_dir_all(&client_dir).expect("create client directory");

    let control = root.path.join("control");
    let inbox = client_dir.join("inbox");
    let outbox = client_dir.join("outbox");

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.current_dir(&supervisor_dir);
    run.env("ANTHROPIC_API_KEY", "test-key");
    run.env("ANTHROPIC_BASE_URL", server.base_url());
    run.env("BATON_MODEL", "model-service");
    run.env("BATON_TIMEOUT_SECS", "5");
    run.env_remove("ANTHROPIC_AUTH_TOKEN");
    run.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    run.env_remove("BATON_EVENT_LOG");
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn baton service run");

    let control_str = control.to_str().unwrap();
    let mut live = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .current_dir(&root.path)
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(live, "baton service run did not report live in time");

    let start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            "inbox",
            "--outbox",
            "outbox",
            "--poll-ms",
            "20",
        ])
        .current_dir(&client_dir)
        .output()
        .expect("run baton service start");
    assert!(
        start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let session_id = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!session_id.is_empty(), "service start prints a session id");
    let mut inbox_ready = false;
    for _ in 0..100 {
        if inbox.is_dir() {
            inbox_ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(inbox_ready, "the session creates its client-relative inbox");
    assert!(
        inbox.is_dir(),
        "relative inbox is created in the client directory"
    );
    assert!(
        !supervisor_dir.join("inbox").exists(),
        "the supervisor directory must not receive the client-relative inbox"
    );

    let status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "status",
            "--control",
            control_str,
            "--session",
            &session_id,
        ])
        .current_dir(&supervisor_dir)
        .output()
        .expect("run baton service status");
    assert!(
        status.status.success(),
        "service status should exit 0; stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("status is JSON");
    let sessions = status_json["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 1, "status reports the started session");
    assert_eq!(sessions[0]["live"], true, "the session reads as live");
    assert_eq!(
        sessions[0]["inbox"],
        inbox.to_str().expect("inbox path is UTF-8")
    );

    let request = MessageEnvelope::new(
        "svc-relative-m1",
        "conv-svc-relative",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "hello",
        1_700_000_000_000,
    );
    mailbox::deliver_to(&inbox, &request).expect("deliver to the client-relative inbox");
    let mut reply = None;
    for _ in 0..100 {
        if let Ok(Some(envelope)) = mailbox::try_claim_response(&outbox, "svc-relative-m1") {
            reply = Some(envelope);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let reply = reply.expect("the session answers through the client-relative outbox");
    assert_eq!(reply.body, "hello from the mock server");
    assert!(
        outbox.is_dir(),
        "the response uses the client-relative outbox"
    );

    let teardown_started = std::time::Instant::now();
    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .current_dir(&client_dir)
        .output()
        .expect("run baton service teardown");
    let teardown_elapsed = teardown_started.elapsed();
    assert!(
        teardown.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    let run_status = run_child.wait().expect("baton service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");
    assert!(
        teardown_elapsed < Duration::from_secs(2),
        "teardown should use cooperative stop, not process-group escalation; took {teardown_elapsed:?}"
    );
}

/// Issue #120 regression: teardown closes the supervisor's admission barrier
/// before it starts draining an in-flight session. The mailbox stop sentinel
/// proves that teardown is spending its bounded stop grace on session A; a
/// second start at that point must fail because `service run` has already
/// released the control lock, so session B cannot become unowned.
#[cfg(target_os = "linux")]
#[test]
fn service_teardown_closes_admission_before_draining_sessions() {
    use baton::mailbox;
    use baton::message::{MessageEnvelope, MessageKind};

    let root = TempMailbox::new("service-teardown-race");
    let control = root.path.join("control");
    let inbox = root.path.join("inbox-a");
    let outbox = root.path.join("outbox-a");
    let racing_inbox = root.path.join("inbox-b");
    let racing_outbox = root.path.join("outbox-b");
    let agent_started = root.path.join("agent-started");

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.env_remove("BATON_EVENT_LOG");
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn baton service run");

    let control_str = control.to_str().unwrap();
    let mut live = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(live, "baton service run did not report live in time");

    // The external agent consumes its request, marks the turn as in-flight,
    // then sleeps long enough for teardown's cooperative-stop window to be
    // observable. This avoids provider/network setup while keeping the real
    // `serve` process blocked in its agent request.
    let start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            inbox.to_str().unwrap(),
            "--outbox",
            outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
            "--agent-cmd",
            "sh",
            "--agent-arg",
            "-c",
            "--agent-arg",
            "cat >/dev/null; touch \"$1\"; sleep 2",
            "--agent-arg",
            "teardown-race-agent",
            "--agent-arg",
            agent_started.to_str().unwrap(),
        ])
        .output()
        .expect("run baton service start");
    assert!(
        start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let session_id = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!session_id.is_empty(), "service start prints a session id");

    let request = MessageEnvelope::new(
        "svc-teardown-race-m1",
        "conv-svc-teardown-race",
        "agent-a",
        "agent-b",
        MessageKind::Request,
        "hold this turn",
        1_700_000_000_000,
    );
    mailbox::deliver_to(&inbox, &request).expect("deliver the in-flight session request");
    let mut in_flight = false;
    for _ in 0..100 {
        if agent_started.is_file() {
            in_flight = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        in_flight,
        "session A did not enter its in-flight agent turn"
    );

    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn baton service teardown");

    // This sentinel marks the beginning of session A's bounded stop grace.
    // The supervisor has already released the control lock by this point, so
    // admission is closed while the session drain continues.
    let session_stop = inbox.join("serve.stop");
    let mut draining = false;
    for _ in 0..250 {
        if session_stop.is_file() {
            draining = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        draining,
        "teardown did not begin draining session A in time"
    );

    let racing_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            racing_inbox.to_str().unwrap(),
            "--outbox",
            racing_outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
        ])
        .output()
        .expect("run racing baton service start");
    assert!(
        !racing_start.status.success(),
        "a racing start must be rejected; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&racing_start.stdout),
        String::from_utf8_lossy(&racing_start.stderr)
    );
    assert!(
        String::from_utf8_lossy(&racing_start.stderr).contains("no live baton service"),
        "the racing start explains that admission is closed: {}",
        String::from_utf8_lossy(&racing_start.stderr)
    );
    assert!(
        String::from_utf8_lossy(&racing_start.stdout)
            .trim()
            .is_empty(),
        "a rejected start prints no session id: {}",
        String::from_utf8_lossy(&racing_start.stdout)
    );

    let teardown_output = teardown
        .wait_with_output()
        .expect("wait for baton service teardown");
    assert!(
        teardown_output.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown_output.stderr)
    );
    let run_status = run_child.wait().expect("service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");

    let status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "status", "--control", control_str])
        .output()
        .expect("run baton service status after teardown");
    assert!(
        status.status.success(),
        "service status should exit 0; stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("status is JSON");
    assert_eq!(
        status_json["service_running"], false,
        "teardown leaves no live service"
    );
    assert_eq!(
        status_json["sessions"].as_array().unwrap().len(),
        0,
        "teardown leaves no managed sessions"
    );
    let session_records = std::fs::read_dir(control.join("sessions"))
        .map(|entries| entries.filter_map(|entry| entry.ok()).count())
        .unwrap_or(0);
    assert_eq!(session_records, 0, "teardown removes every session record");
}

/// Issue #110 AC #1 (task ownership/survival regression): a task started
/// through `baton task start` is spawned as a direct child of the long-lived
/// `baton service run`, not of the short-lived `task start` client that
/// submitted it (which has already exited by the time this test observes the
/// task) — so the service remains the owner and can report the task by id
/// long after the submitting client is gone. Uses trivial real thresholds
/// with no `sleep()` in the test's own assertions: milestone/terminal
/// delivery is awaited via bounded polling, matching every other live-`serve`
/// regression in this file. Linux-only: the parentage proof reads
/// `/proc/<pid>/status`, which has no portable equivalent in this crate.
#[cfg(target_os = "linux")]
#[test]
fn service_task_survives_submitting_client_and_is_owned_by_run() {
    use baton::mailbox;

    let root = TempMailbox::new("task");
    let control = root.path.join("control");
    let session_inbox = root.path.join("session-inbox");
    let session_outbox = root.path.join("session-outbox");
    let callback_inbox = root.path.join("callback");

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.stdout(Stdio::null());
    run.stderr(Stdio::piped());
    let mut run_child = run.spawn().expect("spawn baton service run");
    let run_pid = run_child.id();

    let control_str = control.to_str().unwrap();

    // Wait for `Run` to acquire its control lock: `task start` fails fast
    // rather than waiting when no live service is found yet.
    let mut live = false;
    for _ in 0..200 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    if !live {
        let _ = run_child.kill();
        let output = run_child
            .wait_with_output()
            .expect("collect failed service run output");
        panic!(
            "baton service run did not report live in time; status={:?}; stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Task ownership is tied to an actual managed session, not an arbitrary
    // caller-supplied label. Keep the session alive while the task runs.
    let session_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            session_inbox.to_str().unwrap(),
            "--outbox",
            session_outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
            "--agent-cmd",
            "sh",
            "--agent-arg",
            "-c",
            "--agent-arg",
            "cat >/dev/null; printf ready",
        ])
        .output()
        .expect("run baton service start for task owner");
    assert!(
        session_start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&session_start.stderr)
    );
    let session_id = String::from_utf8_lossy(&session_start.stdout)
        .trim()
        .to_string();
    assert!(!session_id.is_empty(), "service start prints a session id");

    // The short-lived submitting client: `task start` waits for `Run` to
    // spawn and record the task, then exits — there is no lingering process
    // tree behind it to kill.
    let start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "start",
            "--control",
            control_str,
            "--session",
            &session_id,
            "--command",
            "sh",
            "--arg",
            "-c",
            "--arg",
            // Stays alive briefly so the structural PPid check below has a
            // reliable window before the process exits and is reaped —
            // `echo hello` alone completes fast enough to race that check.
            "sleep 0.3; echo hello",
            "--milestone-ms",
            "1",
            "--max-duration-ms",
            "60000",
            "--callback-inbox",
            callback_inbox.to_str().unwrap(),
        ])
        .output()
        .expect("run baton task start");
    assert!(
        start.status.success(),
        "task start should exit 0; stderr: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let task_id = String::from_utf8_lossy(&start.stdout).trim().to_string();
    assert!(!task_id.is_empty(), "task start prints a task id");

    // Structural proof: the task's real PID's parent is `service run`, never
    // the already-exited `task start` client.
    let status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "status",
            "--control",
            control_str,
            "--task",
            &task_id,
        ])
        .output()
        .expect("run baton task status");
    let status_json: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("status is JSON");
    let tasks = status_json["tasks"].as_array().expect("tasks array");
    assert_eq!(tasks.len(), 1, "status reports the started task");
    let task_pid = tasks[0]["pid"].as_u64().expect("pid") as u32;
    assert_eq!(tasks[0]["command"], "sh");
    let stdout_path = PathBuf::from(
        tasks[0]["stdout_path"]
            .as_str()
            .expect("running task stdout path"),
    );
    let stderr_path = PathBuf::from(
        tasks[0]["stderr_path"]
            .as_str()
            .expect("running task stderr path"),
    );
    assert!(stdout_path.is_file(), "running task stdout log exists");
    assert!(stderr_path.is_file(), "running task stderr log exists");
    let ppid = read_ppid(task_pid).expect("the task has a PPid");
    assert_eq!(
        ppid, run_pid,
        "the task's parent is the live `service run`, not the exited submitter"
    );

    // Functional proof: the still-running service delivers both the
    // milestone and terminal lifecycle events to the callback mailbox, with
    // no polling loop on the consumer's part beyond claiming what is already
    // there — the events arrive on the service's own tick, driven by its
    // real (not injectable, in this live-subprocess path) clock, but the
    // test performs no `sleep()` of its own beyond bounded polling for the
    // mailbox to receive them.
    let callback_mailbox = mailbox::Mailbox::open(&callback_inbox).expect("open callback mailbox");
    let mut seen = Vec::new();
    let terminal_key = format!("{task_id}-terminal");
    'poll: for _ in 0..200 {
        // Both events can land in `pending/` together (the same tick both
        // fires the milestone and reaps the already-exited command), and
        // `claim_next` makes no ordering guarantee across distinct ids — so
        // fully drain what is pending each iteration before checking whether
        // the terminal event has been seen, rather than stopping the instant
        // one claim happens to be the terminal one.
        while let Ok(Some(claimed)) = callback_mailbox.claim_next() {
            seen.push(claimed.key.clone());
            callback_mailbox
                .complete(claimed)
                .expect("complete claimed event");
        }
        if seen.contains(&terminal_key) {
            break 'poll;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        seen.contains(&format!("{task_id}-milestone-0")),
        "the configured milestone was delivered to the callback mailbox: {seen:?}"
    );
    assert!(
        seen.contains(&format!("{task_id}-terminal")),
        "the terminal event was delivered to the callback mailbox: {seen:?}"
    );

    // `baton task status` reports the task by id long after the submitting
    // client exited, without a live `Run` loop being required for status.
    let final_status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "task",
            "status",
            "--control",
            control_str,
            "--task",
            &task_id,
        ])
        .output()
        .expect("run baton task status");
    let final_json: serde_json::Value =
        serde_json::from_slice(&final_status.stdout).expect("status is JSON");
    let final_task = &final_json["tasks"][0];
    assert_eq!(final_task["state"], "completed");
    assert_eq!(final_task["command"], "sh");
    assert_eq!(
        final_task["stdout_path"].as_str(),
        stdout_path.to_str(),
        "terminal status preserves stdout path"
    );
    assert_eq!(
        final_task["stderr_path"].as_str(),
        stderr_path.to_str(),
        "terminal status preserves stderr path"
    );
    assert!(stdout_path.is_file(), "terminal task stdout log exists");
    assert!(stderr_path.is_file(), "terminal task stderr log exists");

    // Teardown reaps the task and stops `Run` cooperatively; wait it out.
    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .output()
        .expect("run baton service teardown");
    assert!(
        teardown.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    let run_status = run_child.wait().expect("baton service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");
}

/// Issue #123 regression: task admission rejects both an absent owner record
/// and a record whose session process is already dead, before it creates a
/// child or durable task record.
#[cfg(target_os = "linux")]
#[test]
fn service_task_rejects_missing_and_stale_owners_before_spawn() {
    let root = TempMailbox::new("task-owner-rejection");
    let control = root.path.join("control");
    let stale_inbox = root.path.join("stale-inbox");
    let stale_outbox = root.path.join("stale-outbox");
    let callback_inbox = root.path.join("callback");

    let mut run = Command::new(env!("CARGO_BIN_EXE_baton"));
    run.args(["service", "run", "--control", control.to_str().unwrap()]);
    run.stdout(Stdio::null());
    run.stderr(Stdio::null());
    let mut run_child = run.spawn().expect("spawn baton service run");
    let control_str = control.to_str().unwrap();

    let mut live = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["service", "status", "--control", control_str])
            .output()
            && out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("\"service_running\":true")
        {
            live = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(live, "baton service run did not report live in time");

    let stale_start = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "start",
            "--control",
            control_str,
            "--inbox",
            stale_inbox.to_str().unwrap(),
            "--outbox",
            stale_outbox.to_str().unwrap(),
            "--poll-ms",
            "20",
            "--agent-cmd",
            "sh",
            "--agent-arg",
            "-c",
            "--agent-arg",
            "cat >/dev/null; printf ready",
        ])
        .output()
        .expect("run baton service start for stale owner");
    assert!(
        stale_start.status.success(),
        "service start should exit 0; stderr: {}",
        String::from_utf8_lossy(&stale_start.stderr)
    );
    let stale_session = String::from_utf8_lossy(&stale_start.stdout)
        .trim()
        .to_string();
    assert!(
        !stale_session.is_empty(),
        "service start prints a session id"
    );

    let stale_status = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args([
            "service",
            "status",
            "--control",
            control_str,
            "--session",
            &stale_session,
        ])
        .output()
        .expect("read stale owner status");
    let stale_json: serde_json::Value =
        serde_json::from_slice(&stale_status.stdout).expect("stale owner status is JSON");
    let stale_pid = stale_json["sessions"][0]["pid"]
        .as_u64()
        .expect("stale owner pid") as u32;
    let stale_group = format!("-{stale_pid}");
    let kill = Command::new("kill")
        .args(["-KILL", "--", stale_group.as_str()])
        .status()
        .expect("kill stale owner process group");
    assert!(
        kill.success(),
        "stale owner process group should be killable"
    );

    let mut stale = false;
    for _ in 0..100 {
        if let Ok(out) = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args([
                "service",
                "status",
                "--control",
                control_str,
                "--session",
                &stale_session,
            ])
            .output()
            && out.status.success()
        {
            let json: serde_json::Value =
                serde_json::from_slice(&out.stdout).expect("stale status is JSON");
            stale = json["sessions"]
                .as_array()
                .map(|sessions| sessions.is_empty() || sessions[0]["live"] == false)
                .unwrap_or(true);
            if stale {
                break;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        stale,
        "the killed session should be stale before task admission"
    );

    let cases = [
        (
            "missing-owner".to_string(),
            root.path.join("missing-marker"),
        ),
        (stale_session.clone(), root.path.join("stale-marker")),
    ];
    for (owner, marker) in cases {
        let task_start = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args([
                "task",
                "start",
                "--control",
                control_str,
                "--session",
                owner.as_str(),
                "--command",
                "sh",
                "--arg",
                "-c",
                "--arg",
                "touch \"$1\"; sleep 30",
                "--arg",
                "task-owner",
                "--arg",
                marker.to_str().unwrap(),
                "--max-duration-ms",
                "60000",
                "--callback-inbox",
                callback_inbox.to_str().unwrap(),
            ])
            .output()
            .expect("run baton task start with stale owner");
        assert!(
            !task_start.status.success(),
            "stale owner must be rejected; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&task_start.stdout),
            String::from_utf8_lossy(&task_start.stderr)
        );
        assert!(
            String::from_utf8_lossy(&task_start.stderr)
                .contains("does not name a live managed session"),
            "owner rejection should explain the live-session requirement: {}",
            String::from_utf8_lossy(&task_start.stderr)
        );
        assert!(
            task_start.stdout.is_empty(),
            "rejected task start prints no task id"
        );
        assert!(!marker.exists(), "rejected task must not spawn its command");

        let task_status = Command::new(env!("CARGO_BIN_EXE_baton"))
            .args(["task", "status", "--control", control_str])
            .output()
            .expect("read task status after owner rejection");
        let task_json: serde_json::Value =
            serde_json::from_slice(&task_status.stdout).expect("task status is JSON");
        assert_eq!(
            task_json["tasks"].as_array().unwrap().len(),
            0,
            "owner rejection leaves no durable task record"
        );
    }

    let teardown = Command::new(env!("CARGO_BIN_EXE_baton"))
        .args(["service", "teardown", "--control", control_str])
        .output()
        .expect("run baton service teardown");
    assert!(
        teardown.status.success(),
        "teardown should exit 0; stderr: {}",
        String::from_utf8_lossy(&teardown.stderr)
    );
    let run_status = run_child.wait().expect("baton service run exits");
    assert!(run_status.success(), "service run exits 0 on teardown");
}
