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
