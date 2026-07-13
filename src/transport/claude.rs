//! A non-streaming Claude-compatible Messages client.
//!
//! [`ClaudeClient`] implements [`Transport`] against `POST /v1/messages`. It
//! sends a full conversation history (one or more role-tagged turns) and decodes
//! one assistant reply — no streaming or tool use (those remain out of scope).
//! The request building and response parsing are pure functions so they can be
//! tested without a network via a fake [`HttpClient`].

use serde::{Deserialize, Serialize};

use crate::config::{BatonConfig, Credential};
use crate::error::{BatonError, Result};
use crate::model::{AssistantReply, Message, TokenUsage};
use crate::transport::Transport;
use crate::transport::http::{HttpClient, UreqHttpClient};

/// The Messages API version pinned by this client.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// A Claude-compatible Messages client over an arbitrary [`HttpClient`].
pub struct ClaudeClient<H: HttpClient> {
    config: BatonConfig,
    http: H,
}

impl ClaudeClient<UreqHttpClient> {
    /// Creates a client that talks to the provider over real HTTP, using the
    /// timeout from `config`.
    pub fn from_config(config: BatonConfig) -> Self {
        let http = UreqHttpClient::new(config.timeout);
        Self { config, http }
    }
}

impl<H: HttpClient> ClaudeClient<H> {
    /// Creates a client over a caller-supplied [`HttpClient`].
    ///
    /// Used by tests to inject a fake transport; production code uses
    /// [`ClaudeClient::from_config`].
    pub fn with_http(config: BatonConfig, http: H) -> Self {
        Self { config, http }
    }

    /// The full Messages endpoint URL for the configured base URL.
    fn endpoint(&self) -> String {
        format!("{}/v1/messages", self.config.base_url.trim_end_matches('/'))
    }
}

impl<H: HttpClient> Transport for ClaudeClient<H> {
    fn send_conversation(&self, messages: &[Message]) -> Result<AssistantReply> {
        let body = build_request_body(
            &self.config.model,
            self.config.max_tokens,
            messages,
            self.config.system_prompt.as_deref(),
        )?;
        let url = self.endpoint();
        // `auth_value` is bound to this stack frame so the array of header
        // refs below can borrow from it. The OAuth case formats the bearer
        // token once per request; the API-key case clones the key (also
        // once per request). No heap allocation for the headers themselves.
        let (auth_name, auth_value) = auth_header(&self.config.credential);
        let headers = [
            (auth_name, auth_value.as_str()),
            ("anthropic-version", ANTHROPIC_VERSION),
            ("content-type", "application/json"),
        ];

        let response = self.http.post_json(&url, &headers, &body)?;
        parse_response(response.status, &response.body)
    }
}

/// Maps the resolved [`Credential`] onto the wire-level auth header pair.
///
/// The credential is read from the already-resolved config (no env lookup
/// happens per request) and converted into the matching name/value pair:
/// `ApiKey` -> `x-api-key`, `OAuth` -> `Authorization: Bearer <token>`.
///
/// Returns an owned value for the auth header so it can live on the caller's
/// stack frame and be borrowed into the `&[(&str, &str)]` slice that
/// `HttpClient::post_json` requires.
fn auth_header(credential: &Credential) -> (&'static str, String) {
    match credential {
        Credential::ApiKey(key) => ("x-api-key", key.clone()),
        Credential::OAuth(token) => ("Authorization", format!("Bearer {token}")),
    }
}

/// Serializes a Messages request body for `model` carrying `messages` in order.
///
/// Each turn's [`Role`](crate::model::Role) is emitted as its wire `role` value,
/// preserving order so multi-turn history reaches the provider intact. When
/// `system_prompt` is `Some`, it is emitted as the request's `system` field;
/// `None` omits the field entirely.
fn build_request_body(
    model: &str,
    max_tokens: u32,
    messages: &[Message],
    system_prompt: Option<&str>,
) -> Result<String> {
    let request = MessagesRequest {
        model,
        max_tokens,
        system: system_prompt,
        messages: messages
            .iter()
            .map(|message| RequestMessage {
                role: message.role.as_str(),
                content: &message.content,
            })
            .collect(),
    };
    serde_json::to_string(&request)
        .map_err(|err| BatonError::Transport(format!("failed to serialize request: {err}")))
}

/// Maps an HTTP status and body onto an [`AssistantReply`] or [`BatonError`].
///
/// 2xx responses are decoded into a reply; non-2xx statuses become the matching
/// explicit error variant, surfacing the provider's message rather than hiding
/// the failure.
fn parse_response(status: u16, body: &str) -> Result<AssistantReply> {
    if (200..300).contains(&status) {
        return parse_success(body);
    }

    let message = extract_error_message(body);
    Err(match status {
        401 => BatonError::Auth(message),
        429 => BatonError::RateLimited(message),
        500..=599 => BatonError::Server { status, message },
        _ => BatonError::Api { status, message },
    })
}

/// Decodes a successful Messages response into an [`AssistantReply`].
///
/// All `text` content blocks are concatenated in order. A body that fails to
/// decode, or that carries no assistant text, is a [`BatonError::Decode`] — the
/// client never returns a silently empty reply.
fn parse_success(body: &str) -> Result<AssistantReply> {
    let response: MessagesResponse = serde_json::from_str(body)
        .map_err(|err| BatonError::Decode(format!("malformed Messages response: {err}")))?;

    let text: String = response
        .content
        .iter()
        .filter(|block| block.block_type == "text")
        .filter_map(|block| block.text.as_deref())
        .collect();

    if text.is_empty() {
        return Err(BatonError::Decode(
            "response contained no assistant text".to_string(),
        ));
    }

    // A missing `usage` block (or a missing field within it) is recorded as
    // `None`, not an error — usage is observability, never a decode failure.
    let usage = response
        .usage
        .map_or_else(TokenUsage::default, |u| TokenUsage {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
        });

    Ok(AssistantReply::with_usage(text, usage))
}

/// Pulls `error.message` out of a Claude error body, falling back to the raw
/// body (trimmed) when it is absent or unparseable.
fn extract_error_message(body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<ErrorResponse>(body) {
        return parsed.error.message;
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        "no response body".to_string()
    } else {
        trimmed.to_string()
    }
}

#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    messages: Vec<RequestMessage<'a>>,
}

#[derive(Serialize)]
struct RequestMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct MessagesResponse {
    #[serde(default)]
    content: Vec<ContentBlock>,
    #[serde(default)]
    usage: Option<UsageBlock>,
}

/// The provider's `usage` object. Each count is optional so a partial or absent
/// block degrades to `None` per field rather than failing the decode.
#[derive(Deserialize)]
struct UsageBlock {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Credential, DEFAULT_MAX_TOKENS};
    use crate::model::Prompt;
    use std::cell::RefCell;
    use std::time::Duration;

    /// A fake transport that records the last request and returns a canned
    /// response.
    struct FakeHttp {
        status: u16,
        body: String,
        last_url: RefCell<Option<String>>,
        last_headers: RefCell<Vec<(String, String)>>,
        last_body: RefCell<Option<String>>,
    }

    impl FakeHttp {
        fn new(status: u16, body: &str) -> Self {
            Self {
                status,
                body: body.to_string(),
                last_url: RefCell::new(None),
                last_headers: RefCell::new(Vec::new()),
                last_body: RefCell::new(None),
            }
        }
    }

    impl HttpClient for FakeHttp {
        fn post_json(
            &self,
            url: &str,
            headers: &[(&str, &str)],
            body: &str,
        ) -> Result<crate::transport::http::HttpResponse> {
            *self.last_url.borrow_mut() = Some(url.to_string());
            *self.last_headers.borrow_mut() = headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            *self.last_body.borrow_mut() = Some(body.to_string());
            Ok(crate::transport::http::HttpResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }

    fn config_with(base_url: &str, model: &str) -> BatonConfig {
        config_with_credential(
            base_url,
            model,
            Credential::ApiKey("secret-key".to_string()),
        )
    }

    fn config_with_credential(base_url: &str, model: &str, credential: Credential) -> BatonConfig {
        BatonConfig {
            credential,
            base_url: base_url.to_string(),
            model: model.to_string(),
            timeout: Duration::from_secs(60),
            max_tokens: DEFAULT_MAX_TOKENS,
            system_prompt: None,
        }
    }

    const SUCCESS_BODY: &str = r#"{
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hello there"}],
        "stop_reason": "end_turn"
    }"#;

    #[test]
    fn extracts_assistant_text_from_valid_response() {
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, SUCCESS_BODY),
        );
        let reply = client.send(&Prompt::new("hi")).expect("should succeed");
        assert_eq!(reply.text, "Hello there");
    }

    #[test]
    fn decodes_token_usage_from_response() {
        let body = r#"{
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 12, "output_tokens": 34}
        }"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, body),
        );
        let reply = client.send(&Prompt::new("hi")).expect("should succeed");
        assert_eq!(reply.usage.input_tokens, Some(12));
        assert_eq!(reply.usage.output_tokens, Some(34));
    }

    #[test]
    fn success_without_usage_block_records_absent_tokens() {
        // SUCCESS_BODY carries no `usage`: the reply still succeeds and usage is
        // recorded as unknown (None), never a decode error.
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, SUCCESS_BODY),
        );
        let reply = client.send(&Prompt::new("hi")).expect("should succeed");
        assert_eq!(reply.text, "Hello there");
        assert_eq!(reply.usage.input_tokens, None);
        assert_eq!(reply.usage.output_tokens, None);
    }

    #[test]
    fn partial_usage_block_records_present_field_only() {
        let body = r#"{
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 7}
        }"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, body),
        );
        let reply = client.send(&Prompt::new("hi")).expect("should succeed");
        assert_eq!(reply.usage.input_tokens, Some(7));
        assert_eq!(reply.usage.output_tokens, None);
    }

    #[test]
    fn concatenates_multiple_text_blocks_and_ignores_non_text() {
        let body = r#"{
            "content": [
                {"type": "text", "text": "part one "},
                {"type": "tool_use", "id": "t1", "name": "x", "input": {}},
                {"type": "text", "text": "part two"}
            ]
        }"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-opus-4-8"),
            FakeHttp::new(200, body),
        );
        let reply = client.send(&Prompt::new("hi")).expect("should succeed");
        assert_eq!(reply.text, "part one part two");
    }

    #[test]
    fn request_uses_configured_endpoint_model_key_and_version() {
        let http = FakeHttp::new(200, SUCCESS_BODY);
        // Trailing slash on the base URL must not double up in the path.
        let client = ClaudeClient::with_http(
            config_with("https://proxy.example/", "claude-test-model"),
            http,
        );
        client
            .send(&Prompt::new("hello world"))
            .expect("should succeed");

        let FakeHttp {
            last_url,
            last_headers,
            last_body,
            ..
        } = &client.http;
        assert_eq!(
            last_url.borrow().as_deref(),
            Some("https://proxy.example/v1/messages")
        );

        let headers = last_headers.borrow();
        assert!(headers.contains(&("x-api-key".to_string(), "secret-key".to_string())));
        assert!(headers.contains(&(
            "anthropic-version".to_string(),
            ANTHROPIC_VERSION.to_string()
        )));

        let sent = last_body.borrow();
        let value: serde_json::Value =
            serde_json::from_str(sent.as_deref().unwrap()).expect("body is json");
        assert_eq!(value["model"], "claude-test-model");
        assert_eq!(value["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(value["messages"][0]["role"], "user");
        assert_eq!(value["messages"][0]["content"], "hello world");
    }

    #[test]
    fn request_carries_configured_max_tokens() {
        let mut config = config_with("https://api.anthropic.com", "claude-sonnet-4-6");
        config.max_tokens = 4096;
        let client = ClaudeClient::with_http(config, FakeHttp::new(200, SUCCESS_BODY));
        client.send(&Prompt::new("hi")).expect("should succeed");

        let sent = client.http.last_body.borrow();
        let value: serde_json::Value =
            serde_json::from_str(sent.as_deref().unwrap()).expect("body is json");
        assert_eq!(value["max_tokens"], 4096);
    }

    #[test]
    fn send_conversation_serializes_full_history_in_order() {
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, SUCCESS_BODY),
        );
        let history = [
            Message::user("first"),
            Message::assistant("reply one"),
            Message::user("second"),
        ];
        client.send_conversation(&history).expect("should succeed");

        let sent = client.http.last_body.borrow();
        let value: serde_json::Value =
            serde_json::from_str(sent.as_deref().unwrap()).expect("body is json");
        let messages = value["messages"].as_array().expect("messages is an array");
        assert_eq!(messages.len(), 3, "the full history is sent, got: {value}");
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "first");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "reply one");
        assert_eq!(messages[2]["role"], "user");
        assert_eq!(messages[2]["content"], "second");
    }

    #[test]
    fn request_omits_system_field_when_system_prompt_is_none() {
        let http = FakeHttp::new(200, SUCCESS_BODY);
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            http,
        );
        client.send(&Prompt::new("hi")).expect("should succeed");

        let sent = client.http.last_body.borrow();
        let value: serde_json::Value =
            serde_json::from_str(sent.as_deref().unwrap()).expect("body is json");
        assert!(
            value.get("system").is_none(),
            "system field must be absent when system_prompt is None, got: {value}"
        );
    }

    #[test]
    fn request_includes_system_field_when_system_prompt_is_some() {
        let mut config = config_with("https://api.anthropic.com", "claude-sonnet-4-6");
        config.system_prompt = Some("You are a terse agent.".to_string());
        let client = ClaudeClient::with_http(config, FakeHttp::new(200, SUCCESS_BODY));
        client.send(&Prompt::new("hi")).expect("should succeed");

        let sent = client.http.last_body.borrow();
        let value: serde_json::Value =
            serde_json::from_str(sent.as_deref().unwrap()).expect("body is json");
        assert_eq!(value["system"], "You are a terse agent.");
    }

    #[test]
    fn request_oauth_credential_emits_bearer_header_and_no_api_key() {
        let http = FakeHttp::new(200, SUCCESS_BODY);
        let client = ClaudeClient::with_http(
            config_with_credential(
                "https://api.anthropic.com",
                "claude-sonnet-4-6",
                Credential::OAuth("tok-123".to_string()),
            ),
            http,
        );
        client
            .send(&Prompt::new("hello world"))
            .expect("should succeed");

        let FakeHttp { last_headers, .. } = &client.http;
        let headers = last_headers.borrow();
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "Authorization" && v == "Bearer tok-123"),
            "expected `Authorization: Bearer tok-123` header, got: {headers:?}"
        );
        assert!(
            !headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("x-api-key")),
            "OAuth credential must not emit an `x-api-key` header, got: {headers:?}"
        );
        // The other pinned headers still ride along.
        assert!(headers.contains(&(
            "anthropic-version".to_string(),
            ANTHROPIC_VERSION.to_string()
        )));
        assert!(headers.contains(&("content-type".to_string(), "application/json".to_string())));
    }

    #[test]
    fn unauthorized_maps_to_auth_error() {
        let body = r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(401, body),
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::Auth(msg) => assert_eq!(msg, "invalid x-api-key"),
            other => panic!("expected Auth, got {other:?}"),
        }
    }

    #[test]
    fn too_many_requests_maps_to_rate_limited() {
        let body = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(429, body),
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::RateLimited(msg) => assert_eq!(msg, "slow down"),
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn server_error_maps_to_server_variant_with_status() {
        let body = r#"{"type":"error","error":{"type":"api_error","message":"overloaded"}}"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(503, body),
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::Server { status, message } => {
                assert_eq!(status, 503);
                assert_eq!(message, "overloaded");
            }
            other => panic!("expected Server, got {other:?}"),
        }
    }

    #[test]
    fn other_status_maps_to_api_variant() {
        let body =
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad model"}}"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(400, body),
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::Api { status, message } => {
                assert_eq!(status, 400);
                assert_eq!(message, "bad model");
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[test]
    fn error_body_without_json_falls_back_to_raw_text() {
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(502, "  upstream timeout  "),
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::Server { status, message } => {
                assert_eq!(status, 502);
                assert_eq!(message, "upstream timeout");
            }
            other => panic!("expected Server, got {other:?}"),
        }
    }

    #[test]
    fn malformed_success_body_is_decode_error() {
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, "not json"),
        );
        assert!(matches!(
            client.send(&Prompt::new("hi")).unwrap_err(),
            BatonError::Decode(_)
        ));
    }

    #[test]
    fn success_with_no_text_blocks_is_decode_error() {
        let body = r#"{"content": [{"type": "tool_use", "id": "t1", "name": "x", "input": {}}]}"#;
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FakeHttp::new(200, body),
        );
        assert!(matches!(
            client.send(&Prompt::new("hi")).unwrap_err(),
            BatonError::Decode(_)
        ));
    }

    /// A fake transport that always returns a transport-level error.
    struct FailingHttp;

    impl HttpClient for FailingHttp {
        fn post_json(
            &self,
            _url: &str,
            _headers: &[(&str, &str)],
            _body: &str,
        ) -> Result<crate::transport::http::HttpResponse> {
            Err(BatonError::Transport("connection timed out".to_string()))
        }
    }

    #[test]
    fn timeout_transport_error() {
        let client = ClaudeClient::with_http(
            config_with("https://api.anthropic.com", "claude-sonnet-4-6"),
            FailingHttp,
        );
        match client.send(&Prompt::new("hi")).unwrap_err() {
            BatonError::Transport(msg) => assert!(msg.contains("timed out")),
            other => panic!("expected Transport, got {other:?}"),
        }
    }
}
