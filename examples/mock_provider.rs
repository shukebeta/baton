//! A loopback mock of the Claude Messages API for the quickstart demo.
//!
//! This is the operator-facing twin of the in-process `MockServer` in
//! `tests/integration_test.rs`: a std-only `TcpListener` on `127.0.0.1:0` that
//! answers every connection with the same canned `messages` success body, so
//! `baton converse` / `baton serve` can be driven end-to-end with no API key and
//! no external network. It is a demo/test fixture, not a shipped binary — it
//! lives under `examples/` and is launched by `scripts/quickstart.sh`.
//!
//! Because the port is kernel-assigned (`:0`), the chosen `http://127.0.0.1:<port>`
//! base URL is published two ways so a shell caller can learn it without racing:
//! written to the `--addr-file <path>` file (if given) and printed to stdout.
//! The process serves until killed.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

/// A canned, well-formed Claude Messages response. The exact shape the real
/// transport decodes (`content[].text`, `stop_reason`, `usage`); the body text
/// is fixed because a mock proves plumbing, not conversation quality.
const REPLY_BODY: &str = r#"{
    "id": "msg_mock",
    "type": "message",
    "role": "assistant",
    "content": [{"type": "text", "text": "Hello from the baton mock provider."}],
    "stop_reason": "end_turn",
    "usage": {"input_tokens": 1, "output_tokens": 1}
}"#;

fn main() {
    // Optional `--addr-file <path>`: where to publish the chosen base URL.
    let mut addr_file: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr-file" => {
                addr_file = args.next().or_else(|| {
                    eprintln!("--addr-file requires a value");
                    std::process::exit(2);
                });
            }
            other if other.starts_with("--addr-file=") => {
                addr_file = Some(other["--addr-file=".len()..].to_string());
            }
            other => {
                eprintln!("mock_provider: unexpected argument {other:?}");
                std::process::exit(2);
            }
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock provider");
    let addr = listener.local_addr().expect("read local_addr");
    let base_url = format!("http://{addr}");

    // Publish the URL to the addr-file first (the file's existence is the
    // caller's readiness signal), then to stdout for a human watcher.
    if let Some(path) = addr_file.as_deref() {
        std::fs::write(path, &base_url).expect("write --addr-file");
    }
    println!("{base_url}");
    let _ = std::io::stdout().flush();

    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         content-type: application/json\r\n\
         content-length: {}\r\n\
         connection: close\r\n\
         \r\n\
         {REPLY_BODY}",
        REPLY_BODY.len(),
    );

    // One request/response per connection (the response sets `connection:
    // close`), looping until the process is killed.
    for conn in listener.incoming() {
        let Ok(mut stream) = conn else { break };
        // Drain the request so the client's write completes, then answer. We
        // don't parse the request — every call gets the same canned reply.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let mut buf = [0u8; 8192];
        let _ = stream.read(&mut buf);
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }
}
