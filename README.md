# Baton

Baton is a Rust-based agent harness focused on making AI-to-AI communication
more reliable, structured, and efficient.

Human intervention remains available, but human-first interaction is not the
center of the design.

## Status

The current blessed release is `v0.3.3`.

Early scaffolding. The crate establishes the module layout and typed runtime
shape around a non-streaming Claude-compatible Messages client
(`transport::claude::ClaudeClient`). Its commands wire it to the command line:
`baton ask` for a single-turn first-prompt / first-reply, `baton session` for an
interactive multi-turn REPL that accumulates conversation history across turns,
`baton exchange` for one structured `baton.message/v1` request/reply round-trip,
`baton converse` for a governed two-participant conversation driven to a terminal
condition, `baton converse-ring` for the N-party round-robin generalisation over a
static routing registry, `baton serve` for answering `baton.message/v1` requests
from a file mailbox, `baton send` for posting a request into a mailbox (by path or
by **role name** via the registry) and consuming the correlated reply, `baton
status` for reporting a mailbox's liveness (`idle-done` / `busy` / `crashed-stale`
plus queue depth), and `baton log` for inspecting and replaying the recorded
exchange trail. The surface also includes `baton roles` and `baton role show`
for role identity inspection, `baton service` for host-owned mailbox
supervision, and `baton task` for asynchronous jobs managed by that service.

## Documentation

Everything past this page is reference. Start with the map, then read the one
page for what you are doing.

**Concepts**

- [docs/architecture.md](docs/architecture.md) — read this first: what Baton is,
  the **two participant paths** (external-agent wrapper vs. Baton-owned Messages
  client), the module layout, and the CLI-verb → A2A-model map.
- [docs/protocol.md](docs/protocol.md) — read this when you serialize against
  Baton: the `baton.message/v1` envelope, the `baton.exchange/v1` event schema and
  its nesting, the trail JSONL / replay / merge semantics, and the `baton log`
  verbs that read them.

**Using the CLI**

- [docs/configuration.md](docs/configuration.md) — read this when you configure a
  process: the environment-variable table, role homes (`roles/<name>/`), per-role
  session recording, and the provider transport those settings drive.
- [docs/conversations.md](docs/conversations.md) — read this when you drive a
  conversation: `ask`, `session` (and `--resume`), `exchange`, `converse`, and
  `converse-ring`.
- [docs/mailbox.md](docs/mailbox.md) — read this when agents talk asynchronously:
  `serve`, `send`, `status`, the delivery/at-least-once contract, and the routing
  registry.
- [docs/external-agent.md](docs/external-agent.md) — read this when a mailbox seat
  should be a full-tooled agent CLI rather than one provider call
  (`serve --agent-cmd`).
- [docs/service.md](docs/service.md) — read this when a `serve` session must
  outlive the process that launched it: `baton service` ownership, control
  surface, lifecycle, and systemd/launchd setup.

**Project**

- [docs/versioning.md](docs/versioning.md) — read this before cutting or pinning a
  release: the automated release calculation and the no-retagging baseline.
- [docs/development.md](docs/development.md) — read this before opening a PR: the
  CI gates to run locally.

Run `baton --help` (or `baton -h`) for the current command synopsis. Run
`baton --version` (or `baton -V`) to print the installed crate version. These
global flags need no Baton configuration or provider credentials.

## Install

The primary install path is a prebuilt, checksummed archive from the current
blessed release. Release assets use this constructible pattern:

```text
https://github.com/shukebeta/baton/releases/download/v<version>/baton-<version>-<target>.<archive>
```

Supported targets are `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`x86_64-apple-darwin`, `aarch64-apple-darwin`, and
`x86_64-pc-windows-msvc`. Set `<version>` to the release shown in the Status
section and choose `.tar.gz` for Unix targets or `.zip` for Windows. Each
archive contains only `baton` (or `baton.exe`) at its root.

For example, from a shell with `curl`, `sha256sum`, and the appropriate archive
tool available:

```bash
version="<version>"
target="<target>"
archive="baton-${version}-${target}.tar.gz" # use .zip for Windows
base_url="https://github.com/shukebeta/baton/releases/download/v${version}"

curl --fail --location --remote-name "${base_url}/${archive}"
curl --fail --location --remote-name "${base_url}/SHA256SUMS"
grep -F "  ${archive}" SHA256SUMS | sha256sum --check -
tar -xzf "${archive}" # use unzip for the Windows .zip
```

Put the extracted executable on your `PATH`. On macOS, use
`shasum -a 256` in place of `sha256sum` when checking the selected checksum.

If a Rust toolchain (≥ 1.89) is available, the from-source alternative is:

```bash
cargo install --git https://github.com/shukebeta/baton --tag v0.3.3 --locked
```

This puts `baton` on your PATH. The `--locked` flag is **required**: without it
`cargo install --git` ignores the tracked `Cargo.lock` and resolves fresh
dependency versions, losing the reproducibility the lockfile exists to
guarantee. `--tag <tag>` pins the build to a blessed commit;
`cargo install --git … --rev <sha> --locked` pins just as immutably if you prefer
a raw SHA — the tag is the human-memorable name and GitHub releases anchor over
it.

Consumers stay frozen by pinning a tag, and upgrade by re-pinning a newer tag
deliberately. Pinning is the churn-control mechanism. [`CHANGELOG.md`](CHANGELOG.md)
records what each tag bump includes — read it before re-pinning.

The automated release calculation and its no-retagging baseline are documented
in [docs/versioning.md](docs/versioning.md).

**Historical v0.1.0 baseline.** At the initial release, neither the Rust
library API nor the CLI flag surface was promised stable; the CLI was only the
*intended* integration surface, and pinning a tag was how a consumer insulated
itself from change. That baseline shipped no crates.io publish, no prebuilt or
cross-platform binaries (no homebrew / apt), and no supported library-
dependency recipe — baton compiled as lib+bin, but crate consumption was
unsupported at v0.1.0 because the module layout was intentionally thin and
would be reworked.

## Quickstart

To see the whole A2A loop end-to-end — reproducibly, with no API key and no
external network — run:

```bash
./scripts/quickstart.sh
```

It launches a loopback mock provider (`examples/mock_provider.rs`), points baton
at it via `ANTHROPIC_BASE_URL`, and drives both A2A surfaces:

1. **`baton converse`** — a governed two-agent conversation between the example
   identities in `prompts/interviewer.md` and `prompts/candidate.md`, driven to
   the turn-cap.
2. **`baton serve` + `baton send --await`** — an asynchronous mailbox
   round-trip: `serve` answers a request dropped into an inbox, and `send`
   consumes the correlated reply.

The resulting JSONL trails are written under `target/quickstart/`
(`converse-trail.jsonl` and `serve-send-reply.jsonl`); the script prints each
path and exits 0. It needs only a Rust toolchain — the mock stands in for the
provider, so no credential is read and nothing leaves `127.0.0.1`.

### Mock vs. a real provider

The mock run proves **plumbing and reproducibility**: that the commands wire
together and terminate deterministically. It is *not* a demonstration — every
reply is the same canned line, so a mock-vs-mock exchange is no substitute for
the real artifact.

To **demonstrate** baton to a human, run the same two commands against a real
provider: set a real credential (`ANTHROPIC_API_KEY`), leave `ANTHROPIC_BASE_URL`
at its default (or point it at your gateway), and keep the two distinct system
prompts so the agents hold a genuine conversation with real replies:

```bash
export ANTHROPIC_API_KEY=sk-...          # a real credential
unset ANTHROPIC_BASE_URL                  # use the real Messages API

baton converse \
  --a-system prompts/interviewer.md \
  --b-system prompts/candidate.md \
  --seed "Introduce yourself in one sentence." \
  --out /tmp/trail.jsonl

# In one shell: a long-lived responder.
baton serve --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox
# In another: post a request and read the correlated reply.
baton send --inbox /tmp/mbox/inbox --outbox /tmp/mbox/outbox \
  --await --body "Ping over the mailbox."
```
