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

## Baseline and no-retag boundary

v0.1.0 is the historical baseline. The next feature release from it is v0.2.0;
existing tags are never rewritten. If the release workflow is re-run for a
commit already carrying a valid release tag, it returns that tag without
changing the manifest, lockfile, or commit history.

Release tests cover the baseline boundary, mid-range and carry calculations,
invalid tags and bump kinds, conventional-commit classification, and the
manifest/lockfile/tag consistency contract.
