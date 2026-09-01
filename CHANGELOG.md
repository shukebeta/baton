# Changelog

All notable changes to this project are recorded here.

Baton is installed by pinning a git tag (see [README](README.md#install)), and
stability is an explicit non-goal at 0.1.0 — breaking changes are expected
between tags. This file records what a tag bump includes, so a consumer can
decide whether to re-pin deliberately. Versions do **not** yet follow semantic
versioning.

_Generated from release tags with `bash scripts/release.sh generate-changelog`._

## v0.4.2 … v0.3.51 (2026-09-01)

### Features
- feat(cli): default service control directory (#295)

### Refactors
- refactor(service): share session and task control plumbing (#285)
- refactor(cli): share session spec flag parsing (#286)
- refactor(log): share session outcome pairing (#287)
- refactor(service): share record and channel helpers (#299)

### Other Changes
- design(service): add platform process seam (#297)

## v0.3.50 … v0.3.45 (2026-08-31)

### Fixes
- fix(service): rate-limit grace-wait liveness scans (#269)
- fix(log): warn on uncorrelated session pairing loss (#274)
- fix(transport): preserve Claude stop reason (#276)
- fix(log): preserve session pairing across message paths (#277)
- fix(service): signal groups without PATH dependency (#280)
- fix(service): bound teardown control release wait (#281)

## v0.3.44 … v0.3.36 (2026-08-30)

### Fixes
- fix(task): reject unordered milestones (#248)
- fix(mailbox): clean malformed reply claims (#251)
- fix(task): validate milestone order at service admission (#252)
- fix(quickstart): wait for serve readiness (#255)
- fix(service): cache Linux rehydrated group scans (#258)
- fix(service): probe task record existence without parsing (#260)
- fix(service): cache owned Linux group scans (#262)
- fix(participant): retain stderr on child timeout (#263)
- fix(participant): cap child stdout retention (#267)

## v0.3.35 … v0.3.20 (2026-08-29)

### Fixes
- fix(service): persist Unix session daemon stderr (#227)
- fix(service): gate task test seams from releases (#228)
- fix(session): announce fresh REPL banner (#231)
- fix(service): release the admission lock across stop/teardown grace waits (#221)
- fix(log): pair exchanges by correlation in parse_jsonl (#203) (#232)
- fix(participant): make synthesized reply message_ids unique per emission (#234)
- fix: preserve invalid child output (#238)
- fix(service): skip vanished /proc entries in the group scan (#242)
- fix(service): bounded backoff for milestone delivery, decoupled from supervision (#240)
- fix(windows): escape cmd metacharacters in agent args (#245)

### Docs
- docs(service): add service.probe.lock and undocumented task directories to inventory (#229)
- docs(service): name the session-stopping/ stop-ownership marker (#244)

### Other Changes
- test(log): cover `log merge` trail discovery and warning glue (#208) (#235)
- fix/issue 204 rehydrated probe rate (#233)
- test(serve): cover the `serve --role` composition end-to-end (#239)
- test: make integration deadlines configurable (#241)

## v0.3.19 … v0.3.9 (2026-08-28)

### Fixes
- fix(service/windows): keep direct child's exit outcome across descendant drain (#210) (#211)
- fix(service/windows): recover tasks after supervisor restart (#212)
- fix(service): answer unadmittable start requests with the real reason (#213)
- fix(cli): scope --index to log replay in the help synopsis (#216)
- fix(service): back off stuck terminal callback delivery (#218)
- fix(mailbox): bound the done/ dedup ledger with an operator prune (#217)
- fix(service): drain Unix task process groups (#214)
- fix(windows): preserve backslashes in agent args (#223)

### Docs
- docs(service): clarify start flag scope (#225)
- docs(protocol): complete exchange event table (#226)

### Other Changes
- test(service/windows): cover task admission recovery (#220)

## v0.3.8 (2026-08-27)

### Fixes
- fix(participant): keep quoted program paths intact through cmd /D /S /C on Windows (#197)

## v0.3.7 … v0.3.5 (2026-08-25)

### Fixes
- fix: raise exec-replace poll deadline from 2s to 10s in four service tests (#184)
- fix: retry control-lock acquisition in lock-inheritance test to cover fork-to-exec window (#185)
- fix(serve): capture and persist agent stderr on every turn, not only on failure (#186)

## v0.3.4 … v0.3.3 (2026-08-23)

### Fixes
- fix: use supported Intel macOS runner for releases (#179)

### Other Changes
- ci: migrate actions to Node 24 (#180)

## v0.3.2 … v0.3.1 (2026-08-22)

### Features
- feat(release): generate CHANGELOG.md from release tags (#174)

### Docs
- docs: split README into a landing page plus docs/ topic pages (#177)

### Other Changes
- feat/issue 175 checksummed releases (#178)

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
