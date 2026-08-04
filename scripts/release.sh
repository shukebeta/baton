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
    local tag=""

    while IFS= read -r tag; do
        release_is_valid_tag "${tag}" || continue
        printf '%s\n' "${tag}"
    done < <(git tag --merged HEAD --list 'v*' --sort=-version:refname)
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

release_usage() {
    cat >&2 <<'EOF'
usage:
  scripts/release.sh next-version [latest-tag] [minor|patch]
  scripts/release.sh next-tag [latest-tag] [minor|patch]
  scripts/release.sh create-tag
  scripts/release.sh manifest-version [path]
  scripts/release.sh lockfile-version [path]
  scripts/release.sh verify-docs [manifest] [lockfile] [README]
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
        *)
            release_usage
            return 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    release_main "$@"
fi
