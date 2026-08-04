#!/usr/bin/env bash
# Focused tests for scripts/release.sh.

set -euo pipefail
export BASH_ENV=/dev/null

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../scripts/release.sh
source "${ROOT}/scripts/release.sh"

fail() {
    printf 'FAIL: %s\n' "${1}" >&2
    return 1
}

assert_eq() {
    local expected="${1}" actual="${2}" message="${3:-values differ}"
    [[ "${expected}" == "${actual}" ]] || \
        fail "${message}: expected '${expected}', got '${actual}'"
}

assert_rc_nonzero() {
    local status="${1}"
    (( status != 0 )) || fail "expected a non-zero status"
}

make_fixture() {
    local repo="${1}"

    git -C "${repo}" init -q
    git -C "${repo}" config user.email "release-test@baton.local"
    git -C "${repo}" config user.name "baton release test"
    printf '%s\n' \
        '[package]' \
        'name = "baton"' \
        'version = "0.1.0"' \
        >"${repo}/Cargo.toml"
    printf '%s\n' \
        'version = 4' \
        '' \
        '[[package]]' \
        'name = "baton"' \
        'version = "0.1.0"' \
        'dependencies = []' \
        >"${repo}/Cargo.lock"
    printf '%s\n' \
        "The current blessed release is \`v0.1.0\`." \
        '' \
        'cargo install --git https://github.com/shukebeta/baton --tag v0.1.0 --locked' \
        >"${repo}/README.md"
    git -C "${repo}" add Cargo.toml Cargo.lock README.md
    git -C "${repo}" commit -q -m "chore: release baseline"
    git -C "${repo}" tag v0.1.0
}

test_next_version_baseline_and_empty_repo() (
    set -euo pipefail
    assert_eq "0.2.0" "$(release_next_version v0.1.0 minor)" \
        "feature bump from v0.1.0"
    assert_eq "v0.1.0" "$(release_next_tag "" minor)" \
        "first feature release"
    assert_eq "v0.0.1" "$(release_next_tag "" patch)" \
        "first patch release"
)

test_next_version_mid_range_and_boundaries() (
    set -euo pipefail
    assert_eq "v1.4.0" "$(release_next_tag v1.3.7 minor)" \
        "mid-range feature bump"
    assert_eq "v1.0.0" "$(release_next_tag v0.99.7 minor)" \
        "first major carry"
    assert_eq "v2.0.0" "$(release_next_tag v1.99.7 minor)" \
        "second major carry"
)

test_patch_and_feature_reset_behavior() (
    set -euo pipefail
    assert_eq "v1.99.5" "$(release_next_tag v1.99.4 patch)" \
        "patch bump preserves major and minor"
    assert_eq "v0.5.0" "$(release_next_tag v0.4.7 minor)" \
        "feature bump resets patch"
)

test_conventional_commit_classification() (
    set -euo pipefail
    assert_eq "feat" "$(release_commit_type_for_subject 'feat: add a feature')" \
        "plain feature"
    assert_eq "feat" "$(release_commit_type_for_subject 'feat(core): add a feature')" \
        "scoped feature"
    assert_eq "feat" "$(release_commit_type_for_subject 'feat!: break compatibility')" \
        "breaking feature"
    assert_eq "minor" "$(release_bump_kind_for_subject 'feat(core)!: add a feature')" \
        "scoped breaking feature bump"
    assert_eq "patch" "$(release_bump_kind_for_subject 'BREAKING CHANGE: break compatibility')" \
        "breaking footer does not force a major bump"
    assert_eq "patch" "$(release_bump_kind_for_subject 'fix: correct a bug')" \
        "fix bump"
    assert_eq "patch" "$(release_bump_kind_for_subject 'docs: update the README')" \
        "docs bump"
    assert_eq "patch" "$(release_bump_kind_for_subject 'plain commit subject')" \
        "non-conventional bump"
)

test_invalid_tags_and_bump_kinds_fail() (
    set -euo pipefail
    local status=0

    release_next_tag "not-a-tag" patch >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    status=0
    release_next_tag v0.1 patch >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    status=0
    release_next_tag v0.100.0 patch >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    status=0
    release_next_tag v0.1.0 bogus >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
)

test_release_docs_consistency_and_stale_detection() (
    set -euo pipefail
    local repo status=0
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"

    (cd "${repo}" && release_verify_docs)
    printf '%s\n' \
        "The current blessed release is \`v0.1.1\`." \
        '' \
        'cargo install --git https://github.com/shukebeta/baton --tag v0.1.1 --locked' \
        >"${repo}/README.md"
    (cd "${repo}" && release_verify_docs) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
)

test_existing_v0_1_0_head_is_not_retagged() (
    set -euo pipefail
    local repo before_head after_head before_manifest after_manifest tag_count actual
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"

    before_head="$(git -C "${repo}" rev-parse HEAD)"
    before_manifest="$(<"${repo}/Cargo.toml")"
    actual="$(cd "${repo}" && release_create_tag)"
    after_head="$(git -C "${repo}" rev-parse HEAD)"
    after_manifest="$(<"${repo}/Cargo.toml")"
    tag_count="$(git -C "${repo}" tag --list 'v*' | wc -l | tr -d ' ')"

    assert_eq "v0.1.0" "${actual}" "existing release tag"
    assert_eq "${before_head}" "${after_head}" "no release commit on tagged head"
    assert_eq "${before_manifest}" "${after_manifest}" "manifest unchanged on tagged head"
    assert_eq "1" "${tag_count}" "no duplicate release tag"
)

test_unreachable_higher_tag_is_ignored() (
    set -euo pipefail
    local repo release_branch latest_tag tag version
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"
    release_branch="$(git -C "${repo}" branch --show-current)"

    git -C "${repo}" checkout -q -b unrelated-release-tag
    printf 'unrelated release\n' >"${repo}/unrelated.txt"
    git -C "${repo}" add unrelated.txt
    git -C "${repo}" commit -q -m "chore: unrelated release tag"
    git -C "${repo}" tag v9.99.0
    git -C "${repo}" checkout -q "${release_branch}"

    latest_tag="$(cd "${repo}" && release_latest_tag)"
    assert_eq "v0.1.0" "${latest_tag}" \
        "latest release ignores unrelated branch tags"

    printf 'feature\n' >"${repo}/feature.txt"
    git -C "${repo}" add feature.txt
    git -C "${repo}" commit -q -m "feat: add release behavior"

    tag="$(cd "${repo}" && release_create_tag)"
    version="${tag#v}"
    assert_eq "v0.2.0" "${tag}" "feature release uses reachable baseline"
    assert_eq "${version}" "$(cd "${repo}" && release_manifest_version)" \
        "manifest matches reachable release tag"
    assert_eq "${version}" "$(cd "${repo}" && release_lockfile_version)" \
        "lockfile matches reachable release tag"
    assert_eq "$(git -C "${repo}" rev-parse HEAD)" \
        "$(git -C "${repo}" rev-parse "${tag}^{commit}")" \
        "release tag points at manifest update"
)

test_release_updates_manifest_lock_and_matching_tag() (
    set -euo pipefail
    local repo baseline_head tag tag_head version manifest_version lock_version
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"
    baseline_head="$(git -C "${repo}" rev-parse HEAD)"
    printf 'feature\n' >"${repo}/feature.txt"
    git -C "${repo}" add feature.txt
    git -C "${repo}" commit -q -m "feat: add release behavior"

    tag="$(cd "${repo}" && release_create_tag)"
    version="${tag#v}"
    manifest_version="$(cd "${repo}" && release_manifest_version)"
    lock_version="$(cd "${repo}" && release_lockfile_version)"
    tag_head="$(git -C "${repo}" rev-parse "${tag}^{commit}")"

    assert_eq "v0.2.0" "${tag}" "feature release tag"
    assert_eq "0.2.0" "${version}" "tag version"
    assert_eq "${version}" "${manifest_version}" "manifest matches tag"
    assert_eq "${version}" "${lock_version}" "lockfile matches tag"
    assert_eq "$(git -C "${repo}" rev-parse HEAD)" "${tag_head}" \
        "tag points at manifest update"
    assert_eq "${baseline_head}" "$(git -C "${repo}" rev-parse v0.1.0^{commit})" \
        "historical baseline tag is unchanged"
    git -C "${repo}" show "${tag}:Cargo.toml" | grep -Fx 'version = "0.2.0"' >/dev/null
    git -C "${repo}" show "${tag}:Cargo.lock" | grep -Fx 'version = "0.2.0"' >/dev/null
    git -C "${repo}" show "${tag}:README.md" | grep -Fx "The current blessed release is \`v0.2.0\`." >/dev/null
    git -C "${repo}" show "${tag}:README.md" | grep -Fx 'cargo install --git https://github.com/shukebeta/baton --tag v0.2.0 --locked' >/dev/null
)

test_patch_release_updates_manifest_lock_and_matching_tag() (
    set -euo pipefail
    local repo tag version
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"
    printf 'documentation\n' >"${repo}/docs.txt"
    git -C "${repo}" add docs.txt
    git -C "${repo}" commit -q -m "docs: describe releases"

    tag="$(cd "${repo}" && release_create_tag)"
    version="${tag#v}"

    assert_eq "v0.1.1" "${tag}" "patch release tag"
    assert_eq "${version}" "$(cd "${repo}" && release_manifest_version)" \
        "patch manifest matches tag"
    assert_eq "${version}" "$(cd "${repo}" && release_lockfile_version)" \
        "patch lockfile matches tag"
)

tests=(
    test_next_version_baseline_and_empty_repo
    test_next_version_mid_range_and_boundaries
    test_patch_and_feature_reset_behavior
    test_conventional_commit_classification
    test_invalid_tags_and_bump_kinds_fail
    test_release_docs_consistency_and_stale_detection
    test_existing_v0_1_0_head_is_not_retagged
    test_unreachable_higher_tag_is_ignored
    test_release_updates_manifest_lock_and_matching_tag
    test_patch_release_updates_manifest_lock_and_matching_tag
)

for test_name in "${tests[@]}"; do
    "${test_name}"
    printf 'ok - %s\n' "${test_name}"
done
printf 'release tests: %s passed\n' "${#tests[@]}"
