#!/usr/bin/env bash
#
# external-agent-proof — the manual feasibility proof for issue #68.
#
# Proves that `baton serve --agent-cmd` can host a role backed by a *full-tooled
# native agent CLI run headless* (one that edits files and runs git), driven
# entirely through the file-mailbox with **no tmux and no live TUI**, and that a
# headless-per-message agent reconstructs enough context across rounds — from
# durable artifacts (the git worktree) — to sustain a delivery exchange.
#
# This is an INTEGRATION PROOF, not a hermetic test: it requires a real agent
# CLI and real credentials, so it is NOT part of baton's no-API-key CI. The
# hermetic machinery (envelope wrap, cwd side effect, continuity substrate,
# error paths) is covered by the `ExternalAgentParticipant` unit tests in
# `src/participant.rs`; this script is the real-agent end-to-end.
#
# What it does:
#   1. Creates a throwaway git worktree and starts `baton serve --agent-cmd`
#      pointed at it — the tmux-free launch leaf (no TMAT_PANE, no tmux).
#   2. Round 1: `baton send --await` asks the agent to create notes.md and commit.
#      Asserts an observable side effect (the file + a new commit) AND a
#      well-formed `kind: "response"` reply the sender consumes.
#   3. Round 2: a second addressed message asks the agent to *extend* notes.md.
#      Asserts continuity on a DURABLE ARTIFACT — a further commit and a longer
#      notes.md — proving the round-2 agent reconstructed round 1 from the
#      worktree, not from an in-memory session.
#
# Overrides:
#   AGENT_BIN         native agent CLI to run headless   (default: claude)
#   BATON_BIN         path to the baton binary           (default: target/debug/baton)
#   AGENT_TIMEOUT_MS  per-message agent read timeout      (default: 600000)
#   SEND_TIMEOUT_MS   per-message sender await timeout     (default: 600000)
#
# Credentials: the agent carries its OWN credentials (baton loads no API key in
# --agent-cmd mode). Ensure `$AGENT_BIN` is authenticated in your environment
# before running (e.g. ANTHROPIC_API_KEY / a logged-in `claude`).
#
# The default agent flags use `--dangerously-skip-permissions` so the headless
# agent can edit files and run git non-interactively. This is acceptable ONLY
# because it runs against a throwaway `mktemp -d` git repo with no secrets and no
# network side effects. Edit the `--agent-arg` block below to a narrower policy
# (e.g. --allowedTools) for your own agent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

AGENT_BIN="${AGENT_BIN:-claude}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BATON_BIN="${BATON_BIN:-$TARGET_DIR/debug/baton}"
AGENT_TIMEOUT_MS="${AGENT_TIMEOUT_MS:-600000}"
SEND_TIMEOUT_MS="${SEND_TIMEOUT_MS:-600000}"

# --- Preconditions: a real agent must be present, else skip (not fail) -------
if ! command -v "$AGENT_BIN" >/dev/null 2>&1; then
  echo "external-agent-proof: SKIP — agent CLI '$AGENT_BIN' not found on PATH." >&2
  echo "  This is a manual integration proof; set AGENT_BIN to your agent CLI." >&2
  exit 0
fi

if [[ ! -x "$BATON_BIN" ]]; then
  cargo build --quiet
fi

WORK="$(mktemp -d)"
INBOX="$WORK/mailbox/inbox"
OUTBOX="$WORK/mailbox/outbox"
REPO="$WORK/repo"
mkdir -p "$INBOX" "$OUTBOX" "$REPO"

SERVE_PID=""
cleanup() {
  if [[ -n "$SERVE_PID" ]]; then
    "$BATON_BIN" serve --stop --inbox "$INBOX" >/dev/null 2>&1 || kill "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# --- A throwaway git worktree the agent acts in and reconstructs from --------
git -C "$REPO" init -q
git -C "$REPO" config user.email "proof@baton.local"
git -C "$REPO" config user.name "baton proof"
printf 'seed\n' >"$REPO/README.md"
git -C "$REPO" add README.md
git -C "$REPO" commit -q -m "seed"

ROLE_PROMPT="You are a headless worker agent hosted by baton over a file-mailbox. \
Your current working directory is a git repository. Each message you receive is a \
task. Do the task by editing files in this repository and committing with git, then \
print a single concise line summarising what you did. Reconstruct any earlier \
context from the git history and the files already in the repository."

# --- Launch the tmux-free role host -----------------------------------------
# No TMAT_PANE, no tmux, no live TUI — just `baton serve --agent-cmd`.
echo "external-agent-proof: launching '$AGENT_BIN' as a served role in $REPO"
"$BATON_BIN" serve \
  --inbox "$INBOX" \
  --outbox "$OUTBOX" \
  --agent-cmd "$AGENT_BIN" \
  --agent-cwd "$REPO" \
  --agent-timeout-ms "$AGENT_TIMEOUT_MS" \
  --agent-arg -p \
  --agent-arg --dangerously-skip-permissions \
  --agent-arg --append-system-prompt \
  --agent-arg "$ROLE_PROMPT" &
SERVE_PID=$!

send_round() {
  # send_round <label> <out-file> <body>
  local label="$1" out="$2" body="$3"
  echo "external-agent-proof: $label — sending"
  "$BATON_BIN" send \
    --inbox "$INBOX" \
    --outbox "$OUTBOX" \
    --await \
    --timeout-ms "$SEND_TIMEOUT_MS" \
    --to worker \
    --body "$body" >"$out"
}

fail() { echo "external-agent-proof: FAIL — $1" >&2; exit 1; }

commit_count() { git -C "$REPO" rev-list --count HEAD; }
notes_lines() { [[ -f "$REPO/notes.md" ]] && wc -l <"$REPO/notes.md" | tr -d ' ' || echo 0; }

BASE_COMMITS="$(commit_count)"

# --- Round 1: observable side effect + well-formed reply --------------------
R1_OUT="$WORK/reply-round-1.json"
send_round "round 1" "$R1_OUT" \
  "Create a file named notes.md containing exactly one line: 'round 1 note'. Commit it."

grep -q '"kind":"response"' "$R1_OUT" || fail "round 1 reply is not a well-formed response: $(cat "$R1_OUT")"
[[ -f "$REPO/notes.md" ]] || fail "round 1 produced no notes.md side effect in the worktree"
C1="$(commit_count)"
L1="$(notes_lines)"
[[ "$C1" -gt "$BASE_COMMITS" ]] || fail "round 1 produced no new commit ($C1 <= $BASE_COMMITS)"
echo "external-agent-proof: round 1 OK — notes.md committed ($C1 commits, $L1 line(s)); reply consumed"

# --- Round 2: continuity proven on a durable artifact -----------------------
R2_OUT="$WORK/reply-round-2.json"
send_round "round 2" "$R2_OUT" \
  "Append a SECOND line to the existing notes.md (keep the first line): 'round 2 note'. Commit it."

grep -q '"kind":"response"' "$R2_OUT" || fail "round 2 reply is not a well-formed response: $(cat "$R2_OUT")"
C2="$(commit_count)"
L2="$(notes_lines)"
# Continuity is asserted on the durable artifact, not the free-text reply: a
# further commit AND a longer notes.md means the round-2 agent read round 1's
# file from the worktree and extended it, rather than starting fresh.
[[ "$C2" -gt "$C1" ]] || fail "round 2 produced no further commit ($C2 <= $C1)"
[[ "$L2" -gt "$L1" ]] || fail "round 2 did not extend notes.md ($L2 <= $L1) — no continuity"
echo "external-agent-proof: round 2 OK — notes.md extended to $L2 line(s) across $C2 commits"

echo
echo "external-agent-proof: PASS"
echo "  git log:"
git -C "$REPO" log --oneline | sed 's/^/    /'
echo "  notes.md:"
sed 's/^/    /' "$REPO/notes.md"
