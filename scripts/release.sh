#!/usr/bin/env bash
#
# Baton release helpers. The functions are intentionally sourceable so the
# release workflow and the focused shell tests exercise the same code path.

set -euo pipefail

release_version_regex() {
    printf '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
}

release_tag_regex() {
    printf '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
}

release_validate_version() {
    local version="${1:-}" minor

    [[ "${version}" =~ $(release_version_regex) ]] || {
        printf "release: invalid version '%s'\n" "${version}" >&2
        return 1
    }

    minor="${BASH_REMATCH[2]}"
    (( 10#${minor} < 100 )) || {
        printf "release: minor component must be below 100 in '%s'\n" "${version}" >&2
        return 1
    }
}

release_validate_tag() {
    local tag="${1:-}"

    [[ "${tag}" =~ $(release_tag_regex) ]] || {
        printf "release: invalid release tag '%s'\n" "${tag}" >&2
        return 1
    }
    release_validate_version "${tag#v}"
}

release_is_valid_tag() {
    release_validate_tag "${1:-}" >/dev/null 2>&1
}

release_tags_desc() {
    local tag="" tag_list=""

    # Enumerate before filtering so a failing `git tag` is reported rather than
    # read as an empty list. Callers that generate a file from this list would
    # otherwise mistake a broken lookup for a repository with no releases.
    tag_list="$(git tag --merged HEAD --list 'v*' --sort=-version:refname)" || return 1

    while IFS= read -r tag; do
        release_is_valid_tag "${tag}" || continue
        printf '%s\n' "${tag}"
    done <<< "${tag_list}"
}

release_latest_tag() {
    local tag=""

    while IFS= read -r tag; do
        [[ -n "${tag}" ]] || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(release_tags_desc)
}

release_head_tag() {
    local tag=""

    while IFS= read -r tag; do
        release_is_valid_tag "${tag}" || continue
        printf '%s\n' "${tag}"
        return 0
    done < <(git tag --points-at HEAD --list 'v*' --sort=-version:refname)
}

release_head_subject() {
    git log -1 --format=%s HEAD
}

release_commit_type_for_subject() {
    local subject="${1:-}" type="" regex='^([[:alpha:]]+)(\([^)]+\))?(!)?:[[:space:]]*.+$'

    if [[ "${subject}" =~ ${regex} ]]; then
        type="${BASH_REMATCH[1],,}"
        printf '%s\n' "${type}"
        return 0
    fi

    printf 'other\n'
}

release_bump_kind_for_subject() {
    local subject="${1:-}" type=""
    type="$(release_commit_type_for_subject "${subject}")"

    case "${type}" in
        feat)
            printf 'minor\n'
            ;;
        *)
            printf 'patch\n'
            ;;
    esac
}

release_next_version() {
    local latest_tag="${1:-}" bump_kind="${2:-patch}"
    local version="0.0.0" major=0 minor=0 patch=0

    if [[ -n "${latest_tag}" ]]; then
        release_validate_tag "${latest_tag}" || return 1
        version="${latest_tag#v}"
    fi

    IFS=. read -r major minor patch <<< "${version}"
    major=$((10#${major}))
    minor=$((10#${minor}))
    patch=$((10#${patch}))

    case "${bump_kind}" in
        minor)
            minor=$((minor + 1))
            major=$((major + minor / 100))
            minor=$((minor % 100))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
        *)
            printf "release: unsupported bump kind '%s'\n" "${bump_kind}" >&2
            return 1
            ;;
    esac

    printf '%s.%s.%s\n' "${major}" "${minor}" "${patch}"
}

release_next_tag() {
    local latest_tag="${1:-}" bump_kind="${2:-patch}" version=""
    version="$(release_next_version "${latest_tag}" "${bump_kind}")" || return 1
    printf 'v%s\n' "${version}"
}

release_manifest_version() {
    local manifest_path="${1:-Cargo.toml}"

    [[ -f "${manifest_path}" ]] || {
        printf "release: manifest not found '%s'\n" "${manifest_path}" >&2
        return 1
    }

    awk '
        BEGIN { in_package = 0; found = 0 }
        /^\[package\][[:space:]]*$/ { in_package = 1; next }
        /^\[/ && $0 !~ /^\[package\][[:space:]]*$/ { in_package = 0 }
        in_package && /^version[[:space:]]*=[[:space:]]*"/ {
            line = $0
            sub(/^[^"]*"/, "", line)
            sub(/".*$/, "", line)
            print line
            found = 1
            exit
        }
        END { if (!found) exit 1 }
    ' "${manifest_path}"
}

release_lockfile_version() {
    local lockfile_path="${1:-Cargo.lock}"

    [[ -f "${lockfile_path}" ]] || {
        printf "release: lockfile not found '%s'\n" "${lockfile_path}" >&2
        return 1
    }

    awk '
        BEGIN { in_package = 0; is_baton = 0; found = 0 }
        /^\[\[package\]\]$/ {
            in_package = 1
            is_baton = 0
            next
        }
        in_package && /^name[[:space:]]*=[[:space:]]*"baton"[[:space:]]*$/ {
            is_baton = 1
            next
        }
        in_package && is_baton && /^version[[:space:]]*=[[:space:]]*"/ {
            line = $0
            sub(/^[^"]*"/, "", line)
            sub(/".*$/, "", line)
            print line
            found = 1
            exit
        }
        /^\[/ && $0 !~ /^\[\[package\]\]$/ {
            in_package = 0
            is_baton = 0
        }
        END { if (!found) exit 1 }
    ' "${lockfile_path}"
}

release_readme_install_tag() {
    local readme_path="${1:-README.md}"

    [[ -f "${readme_path}" ]] || {
        printf "release: README not found '%s'\n" "${readme_path}" >&2
        return 1
    }

    awk '
        /cargo install --git https:\/\/github\.com\/shukebeta\/baton --tag v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]* --locked/ {
            match($0, /--tag v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/)
            print substr($0, RSTART + 6, RLENGTH - 6)
            found += 1
        }
        END { if (found != 1) exit 1 }
    ' "${readme_path}"
}

release_readme_current_tag() {
    local readme_path="${1:-README.md}"

    [[ -f "${readme_path}" ]] || {
        printf "release: README not found '%s'\n" "${readme_path}" >&2
        return 1
    }

    awk '
        /^The current blessed release is `v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*`\.$/ {
            match($0, /`v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*`/)
            print substr($0, RSTART + 1, RLENGTH - 2)
            found += 1
        }
        END { if (found != 1) exit 1 }
    ' "${readme_path}"
}

release_set_readme_version() {
    local version="${1:-}" readme_path="${2:-README.md}" tag="" temporary_path=""

    release_validate_version "${version}" || return 1
    [[ -f "${readme_path}" ]] || {
        printf "release: README not found '%s'\n" "${readme_path}" >&2
        return 1
    }
    tag="v${version}"
    temporary_path="$(mktemp)"
    if ! awk -v new_tag="${tag}" '
        BEGIN { install_replaced = 0; current_replaced = 0 }
        {
            line = $0
            if (line ~ /cargo install --git https:\/\/github\.com\/shukebeta\/baton --tag v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]* --locked/) {
                sub(/--tag v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/, "--tag " new_tag, line)
                install_replaced += 1
            }
            if (line ~ /^The current blessed release is `v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*`\.$/) {
                sub(/`v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*`/, "`" new_tag "`", line)
                current_replaced += 1
            }
            print line
        }
        END { if (install_replaced != 1 || current_replaced != 1) exit 1 }
    ' "${readme_path}" >"${temporary_path}"; then
        rm -f "${temporary_path}"
        printf "release: README current-release markers not found exactly once in '%s'\n" \
            "${readme_path}" >&2
        return 1
    fi

    if cmp -s "${temporary_path}" "${readme_path}"; then
        rm -f "${temporary_path}"
    else
        mv "${temporary_path}" "${readme_path}"
    fi
}

release_verify_docs() {
    local manifest_path="${1:-Cargo.toml}" lockfile_path="${2:-Cargo.lock}" readme_path="${3:-README.md}"
    local manifest_version="" lockfile_version="" expected_tag="" install_tag="" current_tag=""

    manifest_version="$(release_manifest_version "${manifest_path}")" || return 1
    lockfile_version="$(release_lockfile_version "${lockfile_path}")" || return 1
    install_tag="$(release_readme_install_tag "${readme_path}")" || return 1
    current_tag="$(release_readme_current_tag "${readme_path}")" || return 1
    expected_tag="v${manifest_version}"

    [[ "${lockfile_version}" == "${manifest_version}" ]] || {
        printf "release: lockfile version '%s' does not match manifest '%s'\n" \
            "${lockfile_version}" "${manifest_version}" >&2
        return 1
    }
    [[ "${install_tag}" == "${expected_tag}" ]] || {
        printf "release: README install tag '%s' does not match '%s'\n" \
            "${install_tag}" "${expected_tag}" >&2
        return 1
    }
    [[ "${current_tag}" == "${expected_tag}" ]] || {
        printf "release: README current-release marker '%s' does not match '%s'\n" \
            "${current_tag}" "${expected_tag}" >&2
        return 1
    }
}

release_set_manifest_version() {
    local version="${1:-}" manifest_path="${2:-Cargo.toml}" temporary_path=""

    release_validate_version "${version}" || return 1
    [[ -f "${manifest_path}" ]] || {
        printf "release: manifest not found '%s'\n" "${manifest_path}" >&2
        return 1
    }

    temporary_path="$(mktemp)"
    if ! awk -v new_version="${version}" '
        BEGIN { in_package = 0; replaced = 0 }
        /^\[package\][[:space:]]*$/ {
            in_package = 1
            print
            next
        }
        /^\[/ && $0 !~ /^\[package\][[:space:]]*$/ { in_package = 0 }
        in_package && /^version[[:space:]]*=[[:space:]]*"/ {
            sub(/"[^"]*"/, "\"" new_version "\"")
            print
            replaced = 1
            next
        }
        { print }
        END { if (!replaced) exit 1 }
    ' "${manifest_path}" >"${temporary_path}"; then
        rm -f "${temporary_path}"
        printf "release: package version not found in '%s'\n" "${manifest_path}" >&2
        return 1
    fi

    if cmp -s "${temporary_path}" "${manifest_path}"; then
        rm -f "${temporary_path}"
    else
        mv "${temporary_path}" "${manifest_path}"
    fi
}

release_set_lockfile_version() {
    local version="${1:-}" lockfile_path="${2:-Cargo.lock}" temporary_path=""

    release_validate_version "${version}" || return 1
    [[ -f "${lockfile_path}" ]] || {
        printf "release: lockfile not found '%s'\n" "${lockfile_path}" >&2
        return 1
    }

    temporary_path="$(mktemp)"
    if ! awk -v new_version="${version}" '
        BEGIN { in_package = 0; is_baton = 0; replaced = 0 }
        /^\[\[package\]\]$/ {
            in_package = 1
            is_baton = 0
            print
            next
        }
        in_package && /^name[[:space:]]*=[[:space:]]*"baton"[[:space:]]*$/ {
            is_baton = 1
            print
            next
        }
        in_package && is_baton && /^version[[:space:]]*=[[:space:]]*"/ {
            sub(/"[^"]*"/, "\"" new_version "\"")
            print
            replaced = 1
            is_baton = 0
            next
        }
        /^\[/ && $0 !~ /^\[\[package\]\]$/ {
            in_package = 0
            is_baton = 0
        }
        { print }
        END { if (!replaced) exit 1 }
    ' "${lockfile_path}" >"${temporary_path}"; then
        rm -f "${temporary_path}"
        printf "release: baton package not found in '%s'\n" "${lockfile_path}" >&2
        return 1
    fi

    if cmp -s "${temporary_path}" "${lockfile_path}"; then
        rm -f "${temporary_path}"
    else
        mv "${temporary_path}" "${lockfile_path}"
    fi
}

release_update_version_files() {
    local version="${1:-}" manifest_path="${2:-Cargo.toml}" lockfile_path="${3:-Cargo.lock}"

    release_set_manifest_version "${version}" "${manifest_path}"
    release_set_lockfile_version "${version}" "${lockfile_path}"
}

# Keep the npm package matrix next to the Rust target matrix. The fields are
# package directory, Rust target, npm os, npm cpu, archive type, and binary
# filename. The publish workflow consumes the same rows through the staging
# and validation functions below.
release_npm_platform_rows() {
    printf '%s\n' \
        'linux-x64|x86_64-unknown-linux-gnu|linux|x64|tar.gz|baton' \
        'linux-arm64|aarch64-unknown-linux-gnu|linux|arm64|tar.gz|baton' \
        'darwin-x64|x86_64-apple-darwin|darwin|x64|tar.gz|baton' \
        'darwin-arm64|aarch64-apple-darwin|darwin|arm64|tar.gz|baton' \
        'win32-x64|x86_64-pc-windows-msvc|win32|x64|zip|baton.exe'
}

release_npm_package_directories() {
    printf 'baton\n'
    while IFS='|' read -r package_key _target _os _cpu _archive _binary; do
        printf 'baton-%s\n' "${package_key}"
    done < <(release_npm_platform_rows)
}

release_npm_shim_path() {
    local release_script_path="${BASH_SOURCE[0]}"

    printf '%s/../packaging/npm/baton.js\n' \
        "$(cd -- "$(dirname -- "${release_script_path}")" && pwd)"
}

release_npm_write_root_manifest() {
    local version="${1:-}"

    release_validate_version "${version}" || return 1
    cat <<EOF
{
  "name": "@shukelabs/baton",
  "version": "${version}",
  "description": "An AI-to-AI harness focused on structured agent communication.",
  "license": "UNLICENSED",
  "bin": {
    "baton": "bin/baton.js"
  },
  "scripts": {
    "install": "node bin/baton.js install --silent"
  },
  "files": [
    "bin"
  ],
  "os": [
    "darwin",
    "linux",
    "win32"
  ],
  "cpu": [
    "x64",
    "arm64"
  ],
  "publishConfig": {
    "access": "public"
  },
  "optionalDependencies": {
    "@shukelabs/baton-linux-x64": "${version}",
    "@shukelabs/baton-linux-arm64": "${version}",
    "@shukelabs/baton-darwin-x64": "${version}",
    "@shukelabs/baton-darwin-arm64": "${version}",
    "@shukelabs/baton-win32-x64": "${version}"
  }
}
EOF
}

release_npm_write_platform_manifest() {
    local version="${1:-}" package_key="${2:-}" npm_os="${3:-}" npm_cpu="${4:-}"

    release_validate_version "${version}" || return 1
    [[ -n "${package_key}" && -n "${npm_os}" && -n "${npm_cpu}" ]] || {
        printf 'release: incomplete npm platform metadata\n' >&2
        return 1
    }
    cat <<EOF
{
  "name": "@shukelabs/baton-${package_key}",
  "version": "${version}",
  "description": "Native Baton binary for ${npm_os}/${npm_cpu}.",
  "license": "UNLICENSED",
  "files": [
    "bin"
  ],
  "os": [
    "${npm_os}"
  ],
  "cpu": [
    "${npm_cpu}"
  ],
  "publishConfig": {
    "access": "public"
  }
}
EOF
}

release_npm_validate_manifest() {
    local manifest_path="${1:-}" expected_name="${2:-}" version="${3:-}"
    local kind="${4:-}" npm_os="${5:-}" npm_cpu="${6:-}"

    [[ -f "${manifest_path}" ]] || {
        printf "release: npm manifest not found '%s'\n" "${manifest_path}" >&2
        return 1
    }
    command -v node >/dev/null 2>&1 || {
        printf 'release: node is required to validate npm manifests\n' >&2
        return 1
    }

    node - "${manifest_path}" "${expected_name}" "${version}" "${kind}" \
        "${npm_os}" "${npm_cpu}" <<'NODE'
const fs = require('node:fs');

const [, , manifestPath, expectedName, expectedVersion, kind, expectedOs, expectedCpu] = process.argv;
let manifest;
try {
  manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
} catch (error) {
  console.error(`release: invalid npm manifest ${manifestPath}: ${error.message}`);
  process.exit(1);
}

function fail(message) {
  console.error(`release: ${manifestPath}: ${message}`);
  process.exit(1);
}

if (manifest.name !== expectedName) fail(`name '${manifest.name}' does not match '${expectedName}'`);
if (manifest.version !== expectedVersion) fail(`version '${manifest.version}' does not match '${expectedVersion}'`);
if (manifest.license !== 'UNLICENSED') fail("license must be UNLICENSED");
if (!Array.isArray(manifest.files) || manifest.files.length !== 1 || manifest.files[0] !== 'bin') {
  fail('files must contain only bin');
}
if (manifest.publishConfig?.access !== 'public') fail('publishConfig.access must be public');
function sameObject(actual, expected) {
  if (!actual || typeof actual !== 'object' || Array.isArray(actual)) return false;
  const actualKeys = Object.keys(actual).sort();
  const expectedKeys = Object.keys(expected).sort();
  return actualKeys.length === expectedKeys.length &&
    actualKeys.every((key, index) => key === expectedKeys[index] && actual[key] === expected[key]);
}

if (kind === 'root') {
  if (manifest.bin?.baton !== 'bin/baton.js') fail('bin.baton must be bin/baton.js');
  if (!sameObject(manifest.scripts, { install: 'node bin/baton.js install --silent' })) {
    fail('scripts must contain only the install lifecycle hook');
  }
  if (JSON.stringify(manifest.os) !== JSON.stringify(['darwin', 'linux', 'win32'])) {
    fail('os matrix is incorrect');
  }
  if (JSON.stringify(manifest.cpu) !== JSON.stringify(['x64', 'arm64'])) {
    fail('cpu matrix is incorrect');
  }
  const expectedDependencies = {
    '@shukelabs/baton-linux-x64': expectedVersion,
    '@shukelabs/baton-linux-arm64': expectedVersion,
    '@shukelabs/baton-darwin-x64': expectedVersion,
    '@shukelabs/baton-darwin-arm64': expectedVersion,
    '@shukelabs/baton-win32-x64': expectedVersion,
  };
  if (!sameObject(manifest.optionalDependencies, expectedDependencies)) {
    fail('optionalDependencies must list all five platform packages at the release version');
  }
} else if (kind === 'platform') {
  if (manifest.scripts) fail('scripts are not allowed in registry packages');
  if (JSON.stringify(manifest.os) !== JSON.stringify([expectedOs])) fail(`os must be ${expectedOs}`);
  if (JSON.stringify(manifest.cpu) !== JSON.stringify([expectedCpu])) fail(`cpu must be ${expectedCpu}`);
} else {
  fail(`unknown manifest kind '${kind}'`);
}
NODE
}

release_npm_validate_package_set() {
    local version="${1:-}" package_root="${2:-}"
    local package_key target npm_os npm_cpu archive binary package_dir
    local entry entry_name file_count

    release_validate_version "${version}" || return 1
    [[ -d "${package_root}" ]] || {
        printf "release: npm package directory not found '%s'\n" "${package_root}" >&2
        return 1
    }

    package_dir="${package_root}/baton"
    release_npm_validate_manifest "${package_dir}/package.json" \
        '@shukelabs/baton' "${version}" root || return 1
    [[ -f "${package_dir}/bin/baton.js" ]] || {
        printf "release: root npm shim not found in '%s'\n" "${package_dir}" >&2
        return 1
    }
    cmp -s "${package_dir}/bin/baton.js" "$(release_npm_shim_path)" || {
        printf "release: staged npm shim differs from packaging/npm/baton.js\n" >&2
        return 1
    }
    file_count="$(find "${package_dir}" -type f | wc -l | tr -d ' ')"
    [[ "${file_count}" == 2 ]] || {
        printf "release: root npm package must contain exactly package.json and bin/baton.js\n" >&2
        return 1
    }

    while IFS='|' read -r package_key target npm_os npm_cpu archive binary; do
        package_dir="${package_root}/baton-${package_key}"
        release_npm_validate_manifest "${package_dir}/package.json" \
            "@shukelabs/baton-${package_key}" "${version}" platform \
            "${npm_os}" "${npm_cpu}" || return 1
        [[ -f "${package_dir}/bin/${binary}" ]] || {
            printf "release: native binary missing from '%s'\n" "${package_dir}" >&2
            return 1
        }
        file_count="$(find "${package_dir}" -type f | wc -l | tr -d ' ')"
        [[ "${file_count}" == 2 ]] || {
            printf "release: npm package '%s' contains unexpected files\n" "${package_dir}" >&2
            return 1
        }
    done < <(release_npm_platform_rows)

    for entry in "${package_root}"/*; do
        [[ -d "${entry}" ]] || {
            printf "release: unexpected file in npm package staging '%s'\n" "${entry}" >&2
            return 1
        }
        entry_name="${entry##*/}"
        case "${entry_name}" in
            baton|baton-linux-x64|baton-linux-arm64|baton-darwin-x64|baton-darwin-arm64|baton-win32-x64) ;;
            *)
                printf "release: unexpected npm package directory '%s'\n" "${entry_name}" >&2
                return 1
                ;;
        esac
    done
}

release_npm_stage_packages() {
    local version="${1:-}" archive_dir="${2:-}" output_dir="${3:-}"
    local staging="" package_key target npm_os npm_cpu archive binary
    local archive_path package_dir extract_dir extracted_binary

    release_validate_version "${version}" || return 1
    [[ -d "${archive_dir}" ]] || {
        printf "release: archive directory not found '%s'\n" "${archive_dir}" >&2
        return 1
    }
    [[ -n "${output_dir}" && ! -e "${output_dir}" ]] || {
        printf "release: npm staging output must be a new path '%s'\n" "${output_dir}" >&2
        return 1
    }
    [[ -f "$(release_npm_shim_path)" ]] || {
        printf 'release: npm shim source is missing\n' >&2
        return 1
    }

    mkdir -p -- "$(dirname -- "${output_dir}")"
    staging="$(mktemp -d "${output_dir}.XXXXXX")" || return 1
    mkdir -p "${staging}/baton/bin"
    cp -- "$(release_npm_shim_path)" "${staging}/baton/bin/baton.js"
    chmod +x "${staging}/baton/bin/baton.js"
    release_npm_write_root_manifest "${version}" >"${staging}/baton/package.json"

    while IFS='|' read -r package_key target npm_os npm_cpu archive binary; do
        archive_path="${archive_dir}/baton-${version}-${target}.${archive}"
        package_dir="${staging}/baton-${package_key}"
        extract_dir="${staging}/.extract-${package_key}"
        [[ -f "${archive_path}" ]] || {
            printf "release: target archive not found '%s'\n" "${archive_path}" >&2
            rm -rf -- "${staging}"
            return 1
        }
        mkdir -p "${package_dir}/bin" "${extract_dir}"
        case "${archive}" in
            tar.gz)
                tar -xzf "${archive_path}" -C "${extract_dir}"
                ;;
            zip)
                unzip -q "${archive_path}" -d "${extract_dir}"
                ;;
            *)
                printf "release: unsupported npm archive type '%s'\n" "${archive}" >&2
                rm -rf -- "${staging}"
                return 1
                ;;
        esac
        extracted_binary="${extract_dir}/${binary}"
        [[ -f "${extracted_binary}" ]] || {
            printf "release: expected binary '%s' missing from '%s'\n" "${binary}" "${archive_path}" >&2
            rm -rf -- "${staging}"
            return 1
        }
        cp -- "${extracted_binary}" "${package_dir}/bin/${binary}"
        [[ "${npm_os}" == 'win32' ]] || chmod +x "${package_dir}/bin/${binary}"
        release_npm_write_platform_manifest "${version}" "${package_key}" \
            "${npm_os}" "${npm_cpu}" >"${package_dir}/package.json"
        rm -rf -- "${extract_dir}"
    done < <(release_npm_platform_rows)

    if ! release_npm_validate_package_set "${version}" "${staging}"; then
        rm -rf -- "${staging}"
        return 1
    fi
    mv -- "${staging}" "${output_dir}"
}

release_sha256_sum() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$@"
    else
        printf 'release: sha256sum or shasum is required\n' >&2
        return 1
    fi
}

release_sha256_check() {
    local checksum_path="${1:-}"

    [[ -f "${checksum_path}" ]] || {
        printf "release: checksum file not found '%s'\n" "${checksum_path}" >&2
        return 1
    }
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum --check "${checksum_path}"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 --check "${checksum_path}"
    else
        printf 'release: sha256sum or shasum is required\n' >&2
        return 1
    fi
}

release_npm_write_checksums() {
    local tarball_dir="${1:-}" checksum_path="${2:-}" checksum_dir="" checksum_name=""
    local tarball=""
    local -a tarballs=() tarball_names=()

    [[ -d "${tarball_dir}" && -n "${checksum_path}" ]] || {
        printf 'release: npm checksum inputs are incomplete\n' >&2
        return 1
    }
    tarballs=("${tarball_dir}"/*.tgz)
    [[ -f "${tarballs[0]}" ]] || {
        printf "release: no npm tarballs found in '%s'\n" "${tarball_dir}" >&2
        return 1
    }
    for tarball in "${tarballs[@]}"; do
        tarball_names+=("${tarball##*/}")
    done
    checksum_dir="$(cd -- "$(dirname -- "${checksum_path}")" && pwd)"
    checksum_name="$(basename -- "${checksum_path}")"
    (cd -- "${tarball_dir}" && release_sha256_sum -- "${tarball_names[@]}") \
        >"${checksum_dir}/${checksum_name}"
    (cd -- "${tarball_dir}" && release_sha256_check "${checksum_dir}/${checksum_name}")
}

release_create_tag() {
    local current_tag="" latest_tag="" subject="" bump_kind="" next_tag="" version=""

    current_tag="$(release_head_tag || true)"
    if [[ -n "${current_tag}" ]]; then
        printf '%s\n' "${current_tag}"
        return 0
    fi

    latest_tag="$(release_latest_tag || true)"
    subject="$(release_head_subject)"
    bump_kind="$(release_bump_kind_for_subject "${subject}")"
    next_tag="$(release_next_tag "${latest_tag}" "${bump_kind}")"
    version="${next_tag#v}"

    if git rev-parse --verify --quiet "refs/tags/${next_tag}" >/dev/null; then
        printf "release: tag '%s' already exists away from HEAD\n" "${next_tag}" >&2
        return 1
    fi

    release_update_version_files "${version}"
    release_set_readme_version "${version}"
    release_verify_docs
    if ! git diff --quiet -- Cargo.toml Cargo.lock README.md; then
        git add Cargo.toml Cargo.lock README.md
        git commit -m "chore(release): ${next_tag} [skip ci]" >/dev/null
    fi

    [[ "$(release_manifest_version)" == "${version}" ]] || {
        printf "release: manifest version does not match '%s'\n" "${next_tag}" >&2
        return 1
    }
    [[ "$(release_lockfile_version)" == "${version}" ]] || {
        printf "release: lockfile version does not match '%s'\n" "${next_tag}" >&2
        return 1
    }

    git tag "${next_tag}" HEAD
    printf '%s\n' "${next_tag}"
}

release_changelog_bucket_for_subject() {
    local subject="${1:-}" type=""
    type="$(release_commit_type_for_subject "${subject}")"

    case "${type}" in
        feat)
            printf 'Features\n'
            ;;
        fix)
            printf 'Fixes\n'
            ;;
        refactor)
            printf 'Refactors\n'
            ;;
        perf)
            printf 'Performance\n'
            ;;
        docs)
            printf 'Docs\n'
            ;;
        *)
            printf 'Other Changes\n'
            ;;
    esac
}

release_render_changelog_section() {
    local title="${1:-}" body="${2:-}"

    [[ -n "${body}" ]] || return 0
    printf '### %s\n%s\n' "${title}" "${body}"
}

# Render the section bodies for one group of release tags sharing a date.
# Buckets accumulate as newline-delimited scalars rather than arrays: a commit
# subject is always exactly one `git log --format=%s` line, so nothing is lost,
# and appending to a scalar needs none of the bash 4.3+ array handling the
# oldest shell in the CI matrix would have to support.
release_changelog_group_body() {
    local previous_tag="${1:-}" first_tag="${2:-}" last_tag="${3:-}"
    local subject="" log_output=""
    local features="" fixes="" refactors="" performance="" docs="" other=""
    local -a log_args=()

    if [[ -n "${previous_tag}" ]]; then
        log_args=("${previous_tag}..${first_tag}")
    else
        # Oldest group, so there is no lower bound to subtract but the group's
        # own oldest tag. Excluding its parents with ^@ rather than ^ keeps a
        # root commit in range: ^@ expands to nothing when there is no parent,
        # where ^ would make git reject the range and silently empty the group.
        log_args=("${first_tag}" --not "${last_tag}^@")
    fi

    # Collect first rather than reading straight from a process substitution:
    # git's status is visible here, so a bad range fails the release job instead
    # of silently rendering the group as empty.
    log_output="$(git log --reverse --format=%s "${log_args[@]}")" || return 1

    while IFS= read -r subject; do
        [[ -n "${subject}" ]] || continue
        case "${subject}" in
            *'[skip ci]'*)
                continue
                ;;
        esac
        case "$(release_changelog_bucket_for_subject "${subject}")" in
            Features)
                features+="- ${subject}"$'\n'
                ;;
            Fixes)
                fixes+="- ${subject}"$'\n'
                ;;
            Refactors)
                refactors+="- ${subject}"$'\n'
                ;;
            Performance)
                performance+="- ${subject}"$'\n'
                ;;
            Docs)
                docs+="- ${subject}"$'\n'
                ;;
            *)
                other+="- ${subject}"$'\n'
                ;;
        esac
    done <<< "${log_output}"

    release_render_changelog_section "Features" "${features}"
    release_render_changelog_section "Fixes" "${fixes}"
    release_render_changelog_section "Refactors" "${refactors}"
    release_render_changelog_section "Performance" "${performance}"
    release_render_changelog_section "Docs" "${docs}"
    release_render_changelog_section "Other Changes" "${other}"
}

# Print one tag group, heading included, or nothing when the group has no
# entries. The leading newline separates this group from whatever precedes it,
# so the document never ends on a blank line.
release_changelog_group() {
    local previous_tag="${1:-}" first_tag="${2:-}" last_tag="${3:-}" release_date="${4:-}"
    local body=""

    body="$(release_changelog_group_body "${previous_tag}" "${first_tag}" "${last_tag}")" || return 1
    [[ -n "${body}" ]] || return 0

    if [[ "${first_tag}" == "${last_tag}" ]]; then
        printf '\n## %s (%s)\n\n%s\n' "${first_tag}" "${release_date}" "${body}"
    else
        printf '\n## %s … %s (%s)\n\n%s\n' "${first_tag}" "${last_tag}" "${release_date}" "${body}"
    fi
}

# Find the newest valid release tag older than the requested tag. The tag's
# own commit is used as the reachability boundary so the result is stable
# while the release workflow has a post-tag changelog commit at HEAD.
release_previous_tag() {
    local target_tag="${1:-}" tag="" tag_list=""

    release_validate_tag "${target_tag}" || return 1
    if ! git rev-parse --verify --quiet "${target_tag}^{commit}" >/dev/null; then
        printf "release: tag '%s' is not present\n" "${target_tag}" >&2
        return 1
    fi

    tag_list="$(git tag --merged "${target_tag}^{commit}" --list 'v*' --sort=-version:refname)" \
        || return 1
    while IFS= read -r tag; do
        release_is_valid_tag "${tag}" || continue
        [[ "${tag}" == "${target_tag}" ]] && continue
        printf '%s\n' "${tag}"
        return 0
    done <<< "${tag_list}"
}

# Render the notes for exactly one tag. This deliberately does not read
# CHANGELOG.md: the release workflow tags the release commit first and adds
# the generated changelog in a later commit, while the notes must describe the
# tagged commit's entries.
release_generate_release_notes() {
    local tag="${1:-}" previous_tag="" release_date="" body=""

    release_validate_tag "${tag}" || return 1
    previous_tag="$(release_previous_tag "${tag}")" || return 1
    release_date="$(git log -1 --format=%cs "${tag}^{commit}")" || return 1
    body="$(release_changelog_group_body "${previous_tag}" "${tag}" "${tag}")" || return 1

    printf '## %s (%s)\n\n' "${tag}" "${release_date}"
    if [[ -n "${body}" ]]; then
        printf '%s\n' "${body}"
    else
        printf 'No notable changes.\n'
    fi
}

release_changelog_preamble() {
    cat <<'EOF'
# Changelog

All notable changes to this project are recorded here.

Baton is installed by pinning a git tag (see [README](README.md#install)), and
stability is an explicit non-goal at 0.1.0 — breaking changes are expected
between tags. This file records what a tag bump includes, so a consumer can
decide whether to re-pin deliberately. Versions do **not** yet follow semantic
versioning.

_Generated from release tags with `bash scripts/release.sh generate-changelog`._
EOF
}

# Walk the release tags newest-first, grouping consecutive tags that share a
# committer date, and print the whole changelog document to stdout.
release_changelog_document() {
    local tag="" tag_date="" group_first="" group_last="" group_date="" any_tag=0
    local tag_list=""

    # Enumerate before rendering anything: reading straight from a process
    # substitution would hide a failing tag lookup as an empty tag list, and an
    # empty changelog would then replace a valid one.
    tag_list="$(release_tags_desc)" || return 1

    release_changelog_preamble

    while IFS= read -r tag; do
        [[ -n "${tag}" ]] || continue
        tag_date="$(git log -1 --format=%cs "${tag}^{commit}")" || return 1

        if [[ -n "${group_first}" && "${tag_date}" == "${group_date}" ]]; then
            group_last="${tag}"
            continue
        fi
        if [[ -n "${group_first}" ]]; then
            release_changelog_group "${tag}" "${group_first}" "${group_last}" "${group_date}" \
                || return 1
        fi
        any_tag=1
        group_first="${tag}"
        group_last="${tag}"
        group_date="${tag_date}"
    done <<< "${tag_list}"

    if [[ -n "${group_first}" ]]; then
        release_changelog_group "" "${group_first}" "${group_last}" "${group_date}" || return 1
    fi

    if (( any_tag == 0 )); then
        printf '\nNo release tags yet.\n'
    fi
}

release_generate_changelog() {
    local output_path="${1:-CHANGELOG.md}" temporary_path=""

    if [[ "${output_path}" == "-" ]]; then
        release_changelog_document || return 1
        return 0
    fi

    # Stage beside the target so the last step is a same-directory rename: the
    # existing changelog is replaced in one operation and stays intact whenever
    # any earlier step fails. This also rejects an unwritable or missing output
    # directory before anything else runs.
    temporary_path="$(mktemp "${output_path}.XXXXXX")" || return 1

    # mktemp creates 0600, and a rename carries that mode onto a tracked file
    # whose permissions git does not restore. Seeding the staging file from the
    # target copies the mode already in effect; with no target yet, fall back to
    # what a plain redirect would have created under the current umask.
    if [[ -f "${output_path}" ]]; then
        if ! cp -p -- "${output_path}" "${temporary_path}"; then
            rm -f -- "${temporary_path}"
            printf "release: cannot read existing changelog '%s'\n" "${output_path}" >&2
            return 1
        fi
    elif ! chmod "$(printf '%o' "$(( 0666 & ~0$(umask) ))")" "${temporary_path}"; then
        rm -f -- "${temporary_path}"
        return 1
    fi

    if ! release_changelog_document >"${temporary_path}"; then
        rm -f -- "${temporary_path}"
        printf "release: failed to generate changelog for '%s'\n" "${output_path}" >&2
        return 1
    fi

    if ! mv -- "${temporary_path}" "${output_path}"; then
        rm -f -- "${temporary_path}"
        printf "release: failed to replace changelog '%s'\n" "${output_path}" >&2
        return 1
    fi
}

release_usage() {
    cat >&2 <<'EOF'
usage:
  scripts/release.sh next-version [latest-tag] [minor|patch]
  scripts/release.sh next-tag [latest-tag] [minor|patch]
  scripts/release.sh create-tag
  scripts/release.sh manifest-version [path]
  scripts/release.sh lockfile-version [path]
  scripts/release.sh verify-docs [manifest] [lockfile] [README]
  scripts/release.sh npm-package-directories
  scripts/release.sh stage-npm-packages <version> <archive-dir> <output-dir>
  scripts/release.sh verify-npm-packages <version> <package-dir>
  scripts/release.sh npm-checksums <tarball-dir> <checksum-path>
  scripts/release.sh generate-changelog [output-path]
  scripts/release.sh generate-release-notes <tag>
EOF
}

release_main() {
    local command="${1:-}"
    shift || true

    case "${command}" in
        next-version)
            release_next_version "${1:-}" "${2:-patch}"
            ;;
        next-tag)
            release_next_tag "${1:-}" "${2:-patch}"
            ;;
        create-tag)
            release_create_tag
            ;;
        manifest-version)
            release_manifest_version "${1:-Cargo.toml}"
            ;;
        lockfile-version)
            release_lockfile_version "${1:-Cargo.lock}"
            ;;
        verify-docs)
            release_verify_docs "${1:-Cargo.toml}" "${2:-Cargo.lock}" "${3:-README.md}"
            ;;
        npm-package-directories)
            release_npm_package_directories
            ;;
        stage-npm-packages)
            release_npm_stage_packages "${1:-}" "${2:-}" "${3:-}"
            ;;
        verify-npm-packages)
            release_npm_validate_package_set "${1:-}" "${2:-}"
            ;;
        npm-checksums)
            release_npm_write_checksums "${1:-}" "${2:-}"
            ;;
        generate-changelog)
            release_generate_changelog "${1:-CHANGELOG.md}"
            ;;
        generate-release-notes)
            release_generate_release_notes "${1:-}"
            ;;
        *)
            release_usage
            return 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    release_main "$@"
fi
