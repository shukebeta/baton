# Release versioning

Baton releases are tagged by [release.yml](../.github/workflows/release.yml),
which uses the sourceable helpers in [release.sh](../scripts/release.sh). The
workflow calculates the next version from the commit that reached main or
master, updates both Cargo.toml and Cargo.lock, commits those files, and tags
that exact commit.

## The rule

The version encodes the cumulative number of released feature commits:

    feature bump: minor += 1; major += minor / 100; minor %= 100; patch = 0
    patch bump:   patch += 1

major * 100 + minor is the feature-count portion of the version. The 100th
feature therefore carries v0.99.x to v1.0.0; the major component is a
mechanical counter, not a declaration of semver compatibility.

Every non-feat conventional-commit type, and every non-conventional subject,
is a patch bump. A feature bump always resets patch to 0.

The classifier recognizes optional scopes and !, so feat(core): ... and feat!:
... both take the feature path. A BREAKING CHANGE: footer does not force a
major bump: it follows the commit's type, and a feat with that footer still
takes the ordinary feature path.

The latest-release lookup considers only valid release tags reachable from the
workflow's current `HEAD` on main or master. Tags that exist only on unrelated
branches do not affect the release sequence.

## README release pin

`README.md` names the current blessed release in its status section and pins
the install command to the same tag. Both markers must match the package
version in `Cargo.toml`, which must also match Baton's entry in `Cargo.lock`.
Run `bash scripts/release.sh verify-docs` to check that contract; CI and the
release workflow run it automatically. When `create-tag` calculates a new
release, it updates both README markers before committing the release files,
so a new tag cannot leave the primary install path on the previous artifact.

## Generated changelog

`CHANGELOG.md` is generated, not hand-maintained. `bash scripts/release.sh
generate-changelog [output-path]` rewrites it from the release tags reachable
from `HEAD`; pass `-` to print to stdout instead. Tags are walked newest-first
and consecutive tags sharing a committer date are merged into one
`## <newest> … <oldest> (<date>)` section, so a day that cut several tags reads
as one release. Commit subjects are bucketed by conventional-commit type into
`Features`, `Fixes`, `Refactors`, `Performance`, `Docs` and `Other Changes`, in
that fixed order, and empty sections are omitted. Any subject containing
`[skip ci]` is dropped, which is what keeps `chore(release):` and
`docs: regenerate changelog` commits from appearing as entries.

The release workflow regenerates the file after `create-tag` and commits the
result as its own `docs: regenerate changelog [skip ci]` commit, created only
when the file actually changed. It cannot be folded into the release commit:
`create-tag` tags `HEAD`, so amending that commit would leave the tag pointing
at an orphan. Because the changelog commit's parent carries the tag, the job
still pushes branch and tag together in its single `git push`. Generation is
idempotent, so a run whose changelog is already current produces no diff and
therefore no commit.

## Baseline and no-retag boundary

v0.1.0 is the historical baseline. The next feature release from it is v0.2.0;
existing tags are never rewritten. If the release workflow is re-run for a
commit already carrying a valid release tag, it returns that tag without
changing the manifest, lockfile, or commit history.

Release tests cover the baseline boundary, mid-range and carry calculations,
invalid tags and bump kinds, conventional-commit classification, the
manifest/lockfile/tag consistency contract, and changelog generation (date
grouping, bucket order, `[skip ci]` filtering, and idempotency).
