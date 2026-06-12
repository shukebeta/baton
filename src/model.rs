//! Typed data structures for the single-turn first-reply flow.
//!
//! These deliberately model only one user prompt and one assistant reply. The
//! epic's non-goals exclude tool calling, streaming, and multi-turn sessions, so
//! there is no message history or role enumeration here yet.

/// A single user prompt to send to the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    /// The prompt text.
    pub text: String,
}

impl Prompt {
    /// Creates a prompt from anything string-like.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// A single assistant reply returned by the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantReply {
    /// The reply text.
    pub text: String,
}

impl AssistantReply {
    /// Creates a reply from anything string-like.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_new_accepts_str_and_string() {
        assert_eq!(Prompt::new("hi"), Prompt::new(String::from("hi")));
        assert_eq!(Prompt::new("hi").text, "hi");
    }

    #[test]
    fn reply_new_stores_text() {
        assert_eq!(AssistantReply::new("ok").text, "ok");
    }
}
