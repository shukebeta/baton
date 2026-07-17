#!/usr/bin/env bash
#
# baton quickstart — run the whole A2A loop end-to-end, reproducibly, with no
# API key and no external network.
#
# It launches a loopback mock provider (examples/mock_provider.rs) and points
# baton at it via ANTHROPIC_BASE_URL, then drives both A2A surfaces:
#   1. `baton converse` — a two-agent (interviewer x candidate) conversation.
#   2. `baton serve` + `baton send --await` — an async mailbox round-trip.
#
# The mock proves plumbing and reproducibility. To *demonstrate* baton to a
# human, run the same two commands against a real provider instead (real
# ANTHROPIC_API_KEY, real ANTHROPIC_BASE_URL, two distinct prompts) — see the
# "Quickstart" section of README.md. A mock-vs-mock exchange is not a substitute
# for the real artifact.
#
# Overrides (used by the CI test so it need not rebuild):
#   BATON_BIN        path to the baton binary       (default target/debug/baton)
#   BATON_MOCK_BIN   path to the mock_provider bin  (default target/debug/examples/mock_provider)
#   QUICKSTART_OUT   durable dir for the trails     (default target/quickstart)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BATON_BIN="${BATON_BIN:-$TARGET_DIR/debug/baton}"
BATON_MOCK_BIN="${BATON_MOCK_BIN:-$TARGET_DIR/debug/examples/mock_provider}"
OUT_DIR="${QUICKSTART_OUT:-$TARGET_DIR/quickstart}"

# Build whatever is missing. A CI test pre-builds and overrides the paths, so
# this is a no-op there; a developer running the script cold gets a build.
if [[ ! -x "$BATON_BIN" ]]; then
  cargo build --quiet
fi
if [[ ! -x "$BATON_MOCK_BIN" ]]; then
  cargo build --quiet --example mock_provider
fi

mkdir -p "$OUT_DIR"
WORK="$(mktemp -d)"

MOCK_PID=""
SERVE_PID=""
cleanup() {
  [[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "$MOCK_PID" ]] && kill "$MOCK_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# --- Launch the loopback mock provider --------------------------------------
ADDR_FILE="$WORK/mock.url"
"$BATON_MOCK_BIN" --addr-file "$ADDR_FILE" >/dev/null &
MOCK_PID=$!

# The addr-file appears once the mock is bound and listening.
for _ in $(seq 1 50); do
  [[ -s "$ADDR_FILE" ]] && break
  sleep 0.1
done
if [[ ! -s "$ADDR_FILE" ]]; then
  echo "quickstart: mock provider did not report its address" >&2
  exit 1
fi
MOCK_URL="$(cat "$ADDR_FILE")"

# Point baton at the mock; keep credential resolution deterministic and bound
# the conversation so it terminates on its own.
export ANTHROPIC_BASE_URL="$MOCK_URL"
export ANTHROPIC_API_KEY="mock-key"
export BATON_MODEL="claude-mock"
export BATON_TIMEOUT_SECS="5"
export BATON_MAX_TURNS="3"
unset ANTHROPIC_AUTH_TOKEN CLAUDE_CODE_OAUTH_TOKEN BATON_TOKEN_BUDGET BATON_EVENT_LOG || true

echo "quickstart: mock provider at $MOCK_URL"

# --- 1. converse: two agents, one governed conversation ---------------------
CONVERSE_TRAIL="$OUT_DIR/converse-trail.jsonl"
"$BATON_BIN" converse \
  --a-system prompts/interviewer.md \
  --b-system prompts/candidate.md \
  --seed "Introduce yourself in one sentence." \
  --out "$CONVERSE_TRAIL"
echo "quickstart: converse trail -> $CONVERSE_TRAIL"

# --- 2. serve + send: an async mailbox round-trip ---------------------------
INBOX="$WORK/mailbox/inbox"
OUTBOX="$WORK/mailbox/outbox"
mkdir -p "$INBOX" "$OUTBOX"

"$BATON_BIN" serve --inbox "$INBOX" --outbox "$OUTBOX" &
SERVE_PID=$!

REPLY_TRAIL="$OUT_DIR/serve-send-reply.jsonl"
# --await prints the correlated reply envelope (one JSON line) to stdout.
"$BATON_BIN" send --inbox "$INBOX" --outbox "$OUTBOX" --await \
  --body "Ping over the mailbox." >"$REPLY_TRAIL"
echo "quickstart: serve+send reply -> $REPLY_TRAIL"

# Cooperative graceful stop, then reap the daemon.
"$BATON_BIN" serve --stop --inbox "$INBOX"
wait "$SERVE_PID" 2>/dev/null || true
SERVE_PID=""

echo "quickstart: done"
