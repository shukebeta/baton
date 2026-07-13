//! Typed data structures for the prompt/reply and multi-turn session flows.
//!
//! [`Prompt`] and [`AssistantReply`] model the single-turn `ask` path. Multi-turn
//! sessions build on [`Message`] (a role-tagged turn) and [`Conversation`] (the
//! accumulated history that is resent with every request). Tool calling and
//! streaming remain out of scope, so a message is plain text with a [`Role`].

/// The author of a single conversation turn.
///
/// Maps 1:1 onto the Messages API `role` field; [`Role::as_str`] is the wire
/// value the transport serializes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A turn authored by the user / calling agent.
    User,
    /// A turn authored by the assistant (a prior reply).
    Assistant,
}

impl Role {
    /// The Messages API wire value for this role.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
    }
}

/// A single role-tagged turn in a conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Who authored this turn.
    pub role: Role,
    /// The turn's text content.
    pub content: String,
}

impl Message {
    /// Creates a user turn from anything string-like.
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
        }
    }

    /// Creates an assistant turn from anything string-like.
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
        }
    }
}

/// An ordered, in-memory accumulation of conversation turns.
///
/// This is the unit-testable core of a multi-turn session: each turn is appended
/// in order, and [`Conversation::messages`] returns the full history that is
/// resent with every request. It deliberately holds no provider state — it is
/// pure data the transport reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Conversation {
    messages: Vec<Message>,
}

impl Conversation {
    /// Creates an empty conversation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a user turn.
    pub fn push_user(&mut self, content: impl Into<String>) {
        self.messages.push(Message::user(content));
    }

    /// Appends an assistant turn.
    pub fn push_assistant(&mut self, content: impl Into<String>) {
        self.messages.push(Message::assistant(content));
    }

    /// Removes and returns the most recent turn, if any.
    ///
    /// Used to roll back a just-appended user turn when its request fails, so
    /// the history never holds two consecutive same-role turns (which the
    /// Messages API rejects).
    pub fn pop(&mut self) -> Option<Message> {
        self.messages.pop()
    }

    /// The full history in order, oldest turn first.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// The number of accumulated turns.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Whether no turns have been accumulated yet.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

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

/// Provider-reported token usage for a single call.
///
/// Each count is optional: a `2xx` response may omit the `usage` block (or a
/// field within it) entirely, in which case that count is `None` (unknown)
/// rather than an error. This is the token-accounting surface the exchange
/// trail records for cost/observability and the future budget governor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    /// Input (prompt) tokens the provider billed, if reported.
    pub input_tokens: Option<u64>,
    /// Output (completion) tokens the provider billed, if reported.
    pub output_tokens: Option<u64>,
}

/// A single assistant reply returned by the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantReply {
    /// The reply text.
    pub text: String,
    /// Provider-reported token usage for the call, when available.
    pub usage: TokenUsage,
}

impl AssistantReply {
    /// Creates a reply from anything string-like, with no usage recorded.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            usage: TokenUsage::default(),
        }
    }

    /// Creates a reply carrying the provider's reported token usage.
    pub fn with_usage(text: impl Into<String>, usage: TokenUsage) -> Self {
        Self {
            text: text.into(),
            usage,
        }
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

    #[test]
    fn role_wire_values() {
        assert_eq!(Role::User.as_str(), "user");
        assert_eq!(Role::Assistant.as_str(), "assistant");
    }

    #[test]
    fn message_constructors_tag_the_role() {
        assert_eq!(
            Message::user("hi"),
            Message {
                role: Role::User,
                content: "hi".to_string(),
            }
        );
        assert_eq!(
            Message::assistant("yo"),
            Message {
                role: Role::Assistant,
                content: "yo".to_string(),
            }
        );
    }

    #[test]
    fn conversation_starts_empty() {
        let convo = Conversation::new();
        assert!(convo.is_empty());
        assert_eq!(convo.len(), 0);
        assert_eq!(convo.messages(), &[]);
    }

    #[test]
    fn conversation_accumulates_turns_in_order() {
        let mut convo = Conversation::new();
        convo.push_user("a");
        convo.push_assistant("b");
        convo.push_user("c");

        assert_eq!(convo.len(), 3);
        assert!(!convo.is_empty());
        assert_eq!(
            convo.messages(),
            &[
                Message::user("a"),
                Message::assistant("b"),
                Message::user("c"),
            ]
        );
    }

    #[test]
    fn conversation_pop_removes_most_recent_turn() {
        let mut convo = Conversation::new();
        convo.push_user("a");
        convo.push_assistant("b");

        assert_eq!(convo.pop(), Some(Message::assistant("b")));
        assert_eq!(convo.messages(), &[Message::user("a")]);
        assert_eq!(convo.pop(), Some(Message::user("a")));
        assert_eq!(convo.pop(), None);
        assert!(convo.is_empty());
    }
}
