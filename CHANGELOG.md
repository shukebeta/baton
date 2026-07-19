# Changelog

All notable changes to this project are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Baton is installed by pinning a git tag (see [README](README.md#install)), and
stability is an explicit non-goal at 0.1.0 — breaking changes are expected
between tags. This file is the curated record of what a tag bump includes, so a
consumer can decide whether to re-pin deliberately. Versions do **not** yet
follow semantic versioning.

Maintenance is manual: land changes under `Unreleased`, then promote that
section under a version heading when a tag is cut.

## [Unreleased]

### Added

- Session-scoped JSONL trail: each `baton session` writes a `session_id` and
  per-turn markers to its trail (#79).
- Resume a prior session from its JSONL trail via `--resume` (#81).
- Per-role home directory (`roles/<name>/`) with layered identity resolution
  (env overrides config) (#83).
- Per-role session recording into `roles/<name>/sessions/` (#85).

### Changed

- Documented the recorded decision that provider configuration stays
  inlined-by-reference (#87).

## [0.1.0] - 2026-07-18

### Added

- Core runtime, configuration, and typed model scaffold (#7).
- Non-streaming Claude Messages client over a `ureq` transport (#9), accepting
  OAuth bearer tokens in addition to API keys (#15).
- `baton ask -p` one-shot CLI command (#11).
- Configurable `BATON_MAX_TOKENS` (#26) and `BATON_SYSTEM_PROMPT` file path
  (#25); startup rejection of `BATON_TIMEOUT_SECS=0` (#31).
- Structured exchange events recorded as JSONL via `BATON_EVENT_LOG` (#17),
  including provider token usage on the `response_ok` event (#44).
- `baton log show` / `baton log replay` for the exchange trail (#28), and
  `baton log merge` for a cross-trail conversation view (#62).
- Multi-turn conversation session (`baton session` REPL) (#27).
- `baton exchange` envelope JSON-in/JSON-out verb (#45).
- `baton.message/v1` A2A envelope nested over `baton.exchange/v1` (#43).
- `baton converse` governed two-participant driver (#48) over a `Participant`
  seam with in-process, subprocess-backed, and mailbox-backed implementations
  (#46, #47, #61).
- `baton converse-ring` N-party round-robin driver (#66) with a to→mailbox
  routing registry (#67).
- `baton serve` file-mailbox for async cross-process delivery (#50) with
  cooperative graceful shutdown (`--stop`) (#60); external-agent participant
  with an output adapter and per-role config (#71, #72).
- `baton send` mailbox client (#59) and `baton status` mailbox liveness (#73).
- GitHub Actions CI workflow (#10) and a pinnable-binary `Install` section in
  the README (#75).
