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

make_npm_archive_fixture() {
    local repo="${1}" version="${2}" package_key target _npm_os _npm_cpu archive binary
    local archive_dir staging archive_path source_windows archive_windows

    archive_dir="${repo}/dist"
    mkdir -p "${archive_dir}"
    while IFS='|' read -r package_key target _npm_os _npm_cpu archive binary; do
        staging="${repo}/staging-${package_key}"
        mkdir -p "${staging}"
        if [[ "${binary}" == 'baton' ]]; then
            printf '#!/bin/sh\nprintf "baton %s\\n"\n' "${version}" >"${staging}/${binary}"
            chmod +x "${staging}/${binary}"
        else
            printf 'fake windows baton %s\n' "${version}" >"${staging}/${binary}"
        fi
        archive_path="${archive_dir}/baton-${version}-${target}.${archive}"
        case "${archive}" in
            tar.gz)
                tar -C "${staging}" -czf "${archive_path}" "${binary}"
                ;;
            zip)
                if command -v zip >/dev/null 2>&1; then
                    (cd "${staging}" && zip -q "${archive_path}" "${binary}")
                elif command -v powershell.exe >/dev/null 2>&1 && command -v cygpath >/dev/null 2>&1; then
                    source_windows="$(cygpath -w "${staging}/${binary}")"
                    archive_windows="$(cygpath -w "${archive_path}")"
                    powershell.exe -NoProfile -NonInteractive -Command \
                        "Compress-Archive -LiteralPath '${source_windows}' -DestinationPath '${archive_windows}' -Force"
                else
                    printf 'npm archive fixture requires zip or PowerShell Compress-Archive\n' >&2
                    return 1
                fi
                ;;
        esac
        rm -rf "${staging}"
    done < <(release_npm_platform_rows)
}

test_npm_platform_matrix_and_staging() (
    set -euo pipefail
    local repo version expected output status host_platform resolved
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    version="0.4.25"

    expected="baton
baton-linux-x64
baton-linux-arm64
baton-darwin-x64
baton-darwin-arm64
baton-win32-x64"
    assert_eq "${expected}" "$(release_npm_package_directories)" \
        "npm package directory matrix"

    make_npm_archive_fixture "${repo}" "${version}"
    release_npm_stage_packages "${version}" "${repo}/dist" "${repo}/npm-packages"
    release_npm_validate_package_set "${version}" "${repo}/npm-packages"

    assert_eq '@shukelabs/baton' \
        "$(node -e 'console.log(require(process.argv[1]).name)' "${repo}/npm-packages/baton/package.json")" \
        "root npm package name"
    assert_eq "${version}" \
        "$(node -e 'console.log(require(process.argv[1]).version)' "${repo}/npm-packages/baton-linux-x64/package.json")" \
        "platform npm package version"

    mkdir -p "${repo}/npm-packages/baton/node_modules/@shukelabs"
    for package_key in linux-x64 linux-arm64 darwin-x64 darwin-arm64 win32-x64; do
        mkdir -p "${repo}/npm-packages/baton/node_modules/@shukelabs/baton-${package_key}/bin"
        cp "${repo}/npm-packages/baton-${package_key}/package.json" \
            "${repo}/npm-packages/baton/node_modules/@shukelabs/baton-${package_key}/package.json"
        cp "${repo}/npm-packages/baton-${package_key}/bin/"* \
            "${repo}/npm-packages/baton/node_modules/@shukelabs/baton-${package_key}/bin/"
    done
    host_platform="$(node -p 'process.platform')"
    if [[ "${host_platform}" == 'win32' ]]; then
        # The fixture's baton.exe is intentionally not a PE binary. On Windows
        # verify the shim's real resolver without trying to execute the text
        # placeholder; package staging above still covers the win32-x64 row.
        resolved="$(node - "${repo}/npm-packages/baton/bin/baton.js" <<'NODE'
const path = require('path');
const { resolvePlatformBinary } = require(process.argv[2]);
const result = resolvePlatformBinary('win32', 'x64');
console.log(result.packageName);
console.log(path.basename(result.binaryPath));
NODE
)"
        assert_eq "@shukelabs/baton-win32-x64
baton.exe" "${resolved}" "Windows npm shim resolves win32-x64 binary"
    else
        output="$(node "${repo}/npm-packages/baton/bin/baton.js" --version)"
        assert_eq "baton ${version}" "${output}" "npm shim forwards to native binary"
    fi

    printf '%s\n' '{"name":"@shukelabs/baton-linux-x64","version":"0.0.1"}' \
        >"${repo}/npm-packages/baton-linux-x64/package.json"
    status=0
    release_npm_validate_package_set "${version}" "${repo}/npm-packages" >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
)

test_npm_pack_checksums() (
    set -euo pipefail
    local repo version package_dir
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    version="0.4.25"
    make_npm_archive_fixture "${repo}" "${version}"
    release_npm_stage_packages "${version}" "${repo}/dist" "${repo}/npm-packages"

    mkdir -p "${repo}/npm-tarballs"
    while read -r package_dir; do
        (cd "${repo}/npm-packages/${package_dir}" && \
            npm pack --ignore-scripts --pack-destination "${repo}/npm-tarballs" >/dev/null)
    done < <(release_npm_package_directories)
    release_npm_write_checksums "${repo}/npm-tarballs" "${repo}/npm-SHA256SUMS"
    (cd "${repo}/npm-tarballs" && release_sha256_check ../npm-SHA256SUMS)
    assert_eq "6" "$(find "${repo}/npm-tarballs" -maxdepth 1 -type f -name '*.tgz' | wc -l | tr -d ' ')" \
        "one npm tarball per package"
)

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

# Commit and tag with pinned dates so the changelog's date grouping is
# deterministic regardless of when or where the suite runs.
changelog_commit() {
    local repo="${1}" date="${2}" subject="${3}"

    printf '%s\n' "${subject}" >>"${repo}/log.txt"
    git -C "${repo}" add log.txt
    GIT_AUTHOR_DATE="${date}T12:00:00+0000" GIT_COMMITTER_DATE="${date}T12:00:00+0000" \
        git -C "${repo}" commit -q -m "${subject}"
}

changelog_tag() {
    local repo="${1}" date="${2}" tag="${3}"

    GIT_COMMITTER_DATE="${date}T12:00:00+0000" git -C "${repo}" tag -f "${tag}" >/dev/null
}

# Baseline v0.1.0, then a 2026-02-01 day carrying v0.2.0 and v0.2.1 with one
# commit per bucket (committed out of bucket order on purpose), then a lone
# v0.2.2 on 2026-03-05.
make_changelog_fixture() {
    local repo="${1}"

    make_fixture "${repo}"
    # make_fixture commits at wall-clock time; re-stamp the baseline so no group
    # date in this fixture depends on the current date.
    GIT_AUTHOR_DATE="2026-01-01T12:00:00+0000" GIT_COMMITTER_DATE="2026-01-01T12:00:00+0000" \
        git -C "${repo}" commit -q --amend --no-edit --date="2026-01-01T12:00:00+0000"
    changelog_tag "${repo}" 2026-01-01 v0.1.0

    changelog_commit "${repo}" 2026-02-01 "docs: describe the first feature"
    changelog_commit "${repo}" 2026-02-01 "feat: add the first feature"
    changelog_commit "${repo}" 2026-02-01 "feat: add the second feature"
    changelog_commit "${repo}" 2026-02-01 "chore(release): v0.2.0 [skip ci]"
    changelog_tag "${repo}" 2026-02-01 v0.2.0
    changelog_commit "${repo}" 2026-02-01 "unconventional subject line"
    changelog_commit "${repo}" 2026-02-01 "perf: speed up the first feature"
    changelog_commit "${repo}" 2026-02-01 "fix: correct the first feature"
    changelog_commit "${repo}" 2026-02-01 "refactor: tidy the first feature"
    changelog_commit "${repo}" 2026-02-01 "docs: regenerate changelog [skip ci]"
    changelog_tag "${repo}" 2026-02-01 v0.2.1

    changelog_commit "${repo}" 2026-03-05 "fix: adjust after the release"
    changelog_tag "${repo}" 2026-03-05 v0.2.2
}

test_generate_changelog_headings_and_tag_grouping() (
    set -euo pipefail
    local repo generated
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_changelog_fixture "${repo}"

    generated="$(cd "${repo}" && release_generate_changelog -)"

    assert_eq "_Generated from release tags with \`bash scripts/release.sh generate-changelog\`._" \
        "$(printf '%s\n' "${generated}" | grep -F 'Generated from release tags')" \
        "generated-from-tags header"

    # Newest-first; same-day tags collapse into one newest-to-oldest heading,
    # and the oldest tag stands alone even though it is a root commit.
    assert_eq "## v0.2.2 (2026-03-05)
## v0.2.1 … v0.2.0 (2026-02-01)
## v0.1.0 (2026-01-01)" \
        "$(printf '%s\n' "${generated}" | grep '^## ')" \
        "tag group headings"
)

test_generate_changelog_bucket_order_and_skip_ci_filtering() (
    set -euo pipefail
    local repo generated group
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_changelog_fixture "${repo}"

    generated="$(cd "${repo}" && release_generate_changelog -)"
    group="$(printf '%s\n' "${generated}" | sed -n '/^## v0.2.1 /,/^## v0.1.0 /p')"

    # Rendered in the fixed order, not the order the commits landed in.
    assert_eq "### Features
### Fixes
### Refactors
### Performance
### Docs
### Other Changes" \
        "$(printf '%s\n' "${group}" | grep '^### ')" \
        "fixed section order within a group"

    assert_eq "- feat: add the first feature
- feat: add the second feature" \
        "$(printf '%s\n' "${group}" | sed -n '/^### Features$/,/^$/p' | grep '^- ')" \
        "feature entries in commit order"
    assert_eq "- unconventional subject line" \
        "$(printf '%s\n' "${group}" | sed -n '/^### Other Changes$/,/^$/p' | grep '^- ')" \
        "non-conventional subject falls into Other Changes"

    assert_eq "" "$(printf '%s\n' "${generated}" | grep -F '[skip ci]' || true)" \
        "release and changelog commits never appear as entries"

    # The lone-tag group has only the one bucket its single commit belongs to.
    assert_eq "### Fixes" \
        "$(printf '%s\n' "${generated}" | sed -n '/^## v0.2.2 /,/^## v0.2.1 /p' | grep '^### ')" \
        "empty sections are omitted"
)

test_generate_changelog_writes_file_idempotently() (
    set -euo pipefail
    local repo generated
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_changelog_fixture "${repo}"

    generated="$(cd "${repo}" && release_generate_changelog -)"
    ( cd "${repo}" && release_generate_changelog CHANGELOG.md )
    assert_eq "${generated}" "$(cat "${repo}/CHANGELOG.md")" \
        "file output matches stdout output"

    ( cd "${repo}" && release_generate_changelog CHANGELOG.md )
    assert_eq "${generated}" "$(cat "${repo}/CHANGELOG.md")" \
        "a second run leaves the file unchanged"

    # A failed generation must not truncate the existing file.
    printf 'sentinel\n' >"${repo}/CHANGELOG.md"
    status=0
    (
        cd "${repo}"
        # shellcheck disable=SC2329  # invoked indirectly by the helper below
        release_changelog_document() { return 1; }
        release_generate_changelog CHANGELOG.md
    ) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    assert_eq "sentinel" "$(cat "${repo}/CHANGELOG.md")" \
        "a failed run leaves the existing changelog intact"
    assert_eq "" "$(cd "${repo}" && find . -maxdepth 1 -name 'CHANGELOG.md.*' -print)" \
        "a failed run leaves no staging file behind"
)

# Make `git tag` fail while every other git subcommand keeps working, so a
# caller can distinguish an unreadable tag list from a tag-less repository.
# Defining it as a shell function shadows the real binary for release.sh, which
# keeps this portable to the Windows leg of the matrix.
fail_git_tag() {
    # shellcheck disable=SC2329  # invoked indirectly, through release.sh
    git() {
        case "${1:-}" in
            tag)
                return 1
                ;;
        esac
        command git "$@"
    }
}

# The final replacement is the only step that can touch the target, so each way
# it can fail must leave the previous changelog readable and unchanged.
test_generate_changelog_failures_preserve_the_existing_file() (
    set -euo pipefail
    local repo status
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_changelog_fixture "${repo}"
    printf 'sentinel\n' >"${repo}/CHANGELOG.md"

    # Failure of the rename itself, i.e. after the content is fully generated.
    status=0
    (
        cd "${repo}"
        # shellcheck disable=SC2329  # shadows the external mv for release.sh
        mv() { return 1; }
        release_generate_changelog CHANGELOG.md
    ) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    assert_eq "sentinel" "$(cat "${repo}/CHANGELOG.md")" \
        "a failed replacement leaves the existing changelog intact"
    assert_eq "" "$(cd "${repo}" && find . -maxdepth 1 -name 'CHANGELOG.md.*' -print)" \
        "a failed replacement leaves no staging file behind"

    # A broken tag lookup must not render an empty changelog over a valid one.
    status=0
    (
        cd "${repo}"
        # shellcheck disable=SC2329  # invoked indirectly by the helper below
        release_tags_desc() { return 1; }
        release_generate_changelog CHANGELOG.md
    ) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    assert_eq "sentinel" "$(cat "${repo}/CHANGELOG.md")" \
        "a failed tag lookup leaves the existing changelog intact"

    # The same must hold when `git tag` itself is what fails, rather than the
    # enumeration helper: an unreadable tag list is not a tag-less repository.
    status=0
    (
        cd "${repo}"
        fail_git_tag
        release_tags_desc
    ) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    status=0
    (
        cd "${repo}"
        fail_git_tag
        release_generate_changelog CHANGELOG.md
    ) >/dev/null 2>&1 || status="$?"
    assert_rc_nonzero "${status}"
    assert_eq "sentinel" "$(cat "${repo}/CHANGELOG.md")" \
        "a failing git tag leaves the existing changelog intact"
    assert_eq "" "$(cd "${repo}" && find . -maxdepth 1 -name 'CHANGELOG.md.*' -print)" \
        "a failing git tag leaves no staging file behind"

    # A missing target directory is refused before anything is staged.
    status=0
    (cd "${repo}" && release_generate_changelog no/such/dir/CHANGELOG.md) >/dev/null 2>&1 \
        || status="$?"
    assert_rc_nonzero "${status}"
)

test_generate_changelog_covers_a_root_commit_in_a_multi_tag_group() (
    set -euo pipefail
    local repo generated
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_fixture "${repo}"
    # The baseline is the root commit; putting a second tag on the same date
    # makes the oldest group span down to a commit with no parent.
    GIT_AUTHOR_DATE="2026-01-01T12:00:00+0000" GIT_COMMITTER_DATE="2026-01-01T12:00:00+0000" \
        git -C "${repo}" commit -q --amend --no-edit --date="2026-01-01T12:00:00+0000"
    changelog_tag "${repo}" 2026-01-01 v0.1.0
    changelog_commit "${repo}" 2026-01-01 "fix: repair the baseline"
    changelog_tag "${repo}" 2026-01-01 v0.1.1

    generated="$(cd "${repo}" && release_generate_changelog -)"

    assert_eq "## v0.1.1 … v0.1.0 (2026-01-01)" \
        "$(printf '%s\n' "${generated}" | grep '^## ')" \
        "root-commit group heading"
    assert_eq "- chore: release baseline" \
        "$(printf '%s\n' "${generated}" | sed -n '/^### Other Changes$/,/^$/p' | grep '^- ')" \
        "the root commit is still listed"
    assert_eq "- fix: repair the baseline" \
        "$(printf '%s\n' "${generated}" | sed -n '/^### Fixes$/,/^$/p' | grep '^- ')" \
        "the non-root commit is still listed"
)

test_generate_release_notes_for_one_tag() (
    set -euo pipefail
    local repo generated
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    make_changelog_fixture "${repo}"

    # v0.2.1 is not HEAD, proving that notes use the requested tag's reachable
    # history rather than the later changelog/tag history at HEAD.
    generated="$(cd "${repo}" && release_generate_release_notes v0.2.1)"
    assert_eq "## v0.2.1 (2026-02-01)" \
        "$(printf '%s\n' "${generated}" | grep '^## ')" \
        "single-tag release-notes heading"
    assert_eq "- fix: correct the first feature
- refactor: tidy the first feature
- perf: speed up the first feature
- unconventional subject line" \
        "$(printf '%s\n' "${generated}" | grep '^- ')" \
        "single-tag release entries"
    assert_eq "" \
        "$(printf '%s\n' "${generated}" | grep -F 'fix: adjust after the release' || true)" \
        "later tag entries stay out of release notes"
    assert_eq "" \
        "$(printf '%s\n' "${generated}" | grep -F '[skip ci]' || true)" \
        "skip-ci entries stay out of release notes"
)

test_generate_changelog_without_release_tags() (
    set -euo pipefail
    local repo
    repo="$(mktemp -d)"
    trap 'rm -rf "${repo}"' EXIT
    git -C "${repo}" init -q
    git -C "${repo}" config user.email "release-test@baton.local"
    git -C "${repo}" config user.name "baton release test"
    changelog_commit "${repo}" 2026-01-01 "feat: untagged work"

    assert_eq "No release tags yet." \
        "$(cd "${repo}" && release_generate_changelog - | grep -F 'No release tags')" \
        "untagged repository renders the empty-changelog notice"
)

tests=(
    test_next_version_baseline_and_empty_repo
    test_next_version_mid_range_and_boundaries
    test_patch_and_feature_reset_behavior
    test_conventional_commit_classification
    test_invalid_tags_and_bump_kinds_fail
    test_npm_platform_matrix_and_staging
    test_npm_pack_checksums
    test_release_docs_consistency_and_stale_detection
    test_existing_v0_1_0_head_is_not_retagged
    test_unreachable_higher_tag_is_ignored
    test_release_updates_manifest_lock_and_matching_tag
    test_patch_release_updates_manifest_lock_and_matching_tag
    test_generate_changelog_headings_and_tag_grouping
    test_generate_changelog_bucket_order_and_skip_ci_filtering
    test_generate_changelog_writes_file_idempotently
    test_generate_changelog_covers_a_root_commit_in_a_multi_tag_group
    test_generate_release_notes_for_one_tag
    test_generate_changelog_failures_preserve_the_existing_file
    test_generate_changelog_without_release_tags
)

for test_name in "${tests[@]}"; do
    "${test_name}"
    printf 'ok - %s\n' "${test_name}"
done
printf 'release tests: %s passed\n' "${#tests[@]}"
