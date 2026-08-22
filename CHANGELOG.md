# Changelog

All notable changes to this project are recorded here.

Baton is installed by pinning a git tag (see [README](README.md#install)), and
stability is an explicit non-goal at 0.1.0 — breaking changes are expected
between tags. This file records what a tag bump includes, so a consumer can
decide whether to re-pin deliberately. Versions do **not** yet follow semantic
versioning.

_Generated from release tags with `bash scripts/release.sh generate-changelog`._

## v0.3.1 (2026-08-22)

### Features
- feat(release): generate CHANGELOG.md from release tags (#174)

### Docs
- docs: split README into a landing page plus docs/ topic pages (#177)

## v0.3.0 (2026-08-10)

### Features
- feat(service): implement Windows service and task ownership (#172)

## v0.2.12 … v0.2.11 (2026-08-09)

### Docs
- docs: document notify message kind (#169)

### Other Changes
- Persist macOS start epoch for legacy records (#171)

## v0.2.10 (2026-08-04)

### Fixes
- fix: synchronize README with released version (#167)

## v0.2.9 … v0.2.1 (2026-08-03)

### Fixes
- fix: make task admission restart-safe (#150)
- fix: stabilize macOS service liveness keys (#153)
- fix: ignore unreachable release tags (#155)
- fix: keep task rollback markers durable through cleanup (#156)
- fix: make task start response consumption restart-safe (#157)
- fix: retain unresolved prepared task admissions (#162)
- fix: avoid signalling terminal task records (#163)
- fix: retain unresolved tasks without grace wait (#165)

### Other Changes
- fix/issue 158 liveness unresolved (#159)

## v0.2.0 (2026-08-02)

### Features
- feat(session): session-scoped JSONL trail (session_id + turn markers) (#79)
- feat(session): resume a prior session from its JSONL trail (--resume) (#81)
- feat(roles): per-role home directory + layered env>config identity resolution (#80) (#83)
- feat(session): per-role session recording into roles/<name>/sessions/ (#82) (#85)
- feat(cli): add global help and version flags (#104)
- feat(task): add service-owned asynchronous jobs for mailbox callbacks (#121)
- feat(release): automate feature-count versioning (#148)

### Fixes
- fix(participant): resolve Windows agent cmd shims (#105)
- fix: make baton a pure backend-agnostic transport (#115)
- fix: allow role-less external serve without home (#124)
- fix: record participant failures in role sessions (#126)
- fix: correlate session outcomes in shared trails (#127)
- fix: terminate process groups without signal broadcast
- fix: reconcile tasks after service restart (#138)
- fix: resolve task paths from submitting client (#135) (#140)
- fix: fail task start after service loss (#141)
- fix: preserve timeout after service restart (#142)

### Docs
- docs(readme): record provider-config decision (inlined-by-reference stays) (#87)
- docs: add CHANGELOG.md with Unreleased + v0.1.0 baseline (#92)
- docs: add docs/ hub + architecture/onboarding overview (#88) (#94)
- docs: extract wire-protocol reference into docs/protocol.md (#89) (#95)
- docs: clarify caller-owned MCP configuration (#128)

### Other Changes
- chore: add LICENSE + Cargo.toml license metadata (#91) (#93)
- ci: add msrv job pinned to rustc 1.89.0 (#97)
- ci: run the `ci` job on macOS and Windows too
- test: serialize converse-ring registry fixtures (#106)
- test: drain mock HTTP request bodies (#107)
- feat/issue 109 host owned service supervisor (#118)
- test: cover external agent argument forwarding (#129)
- fix service start relative session paths (#130)
- fix service teardown admission race (#131)
- fix/issue 123 task owner admission (#133)
- Expose task command and log paths in status (#134)
- test: make task timeout coverage deterministic (#144)
- feat/issue 111 macos launchd (#146)

## v0.1.0 (2026-07-18)

### Docs
- docs(readme): add pinnable binary Install section (v0.1.0 tag) (#75)
