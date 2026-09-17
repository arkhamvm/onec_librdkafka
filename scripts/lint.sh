#!/usr/bin/env bash
#
# Static analysis for the onec-librdkafka component.
#
# Two tools, because they find different things:
#
#   clang-tidy   per-file checks driven by tests/lint/.clang-tidy - ownership,
#                CERT rules, narrowing conversions, unchecked C return values.
#   scan-build   the Clang Static Analyzer over a real cmake build of the
#                component: path-sensitive, cross-function, and the only one of
#                the two that can see a leak or a use-after-free that only
#                happens on one branch.
#
# What is NOT analysed, because we do not own it and a finding there is not
# actionable:
#
#   src/rdkafka.h, src/rdkafkacpp.h   vendored librdkafka headers, kept pristine
#   src/nlohmann/json.hpp             vendored
#   include/                          the 1C Native API SDK
#
# Usage:
#   scripts/lint.sh                 report only
#   scripts/lint.sh --fix           apply clang-tidy's automatic fixes
#   scripts/lint.sh --strict        advisory findings also fail the run
#   scripts/lint.sh --tidy-only     skip scan-build (much faster)
#   scripts/lint.sh --scan-only     skip clang-tidy
#   scripts/lint.sh -j 4            parallel clang-tidy jobs (default: nproc)
#
# --fix records every replacement as YAML first and merges the lot with
# clang-apply-replacements at the end, rather than letting eighteen translation
# units rewrite the same ten headers underneath each other. It then REBUILDS the
# component and fails if the result no longer compiles, because a fix pass that
# breaks the build is worse than no fix pass. Two checks are reported but never
# auto-fixed; FIX_EXCLUDED_CHECKS below says which and why.
#
# THE GATE. The exit status is what makes this usable in a pre-push hook, so it
# has to mean something precise:
#
#   BLOCKING - clang-tidy diagnostics at severity "error", every
#              clang-analyzer-* finding, and every scan-build report. These are
#              "this code is wrong on some path", they have close to no false
#              positives, and there are ZERO of them on the current tree. That
#              is the whole point: a gate that is red the day it lands gets
#              switched off within a week.
#   ADVISORY - everything else the configuration enables: ownership, CERT,
#              performance, narrowing. Around a hundred on the current tree,
#              printed and counted, but they do not fail the run unless
#              --strict is given.
#
# Exit codes:
#   0  no blocking findings (and, with --strict, no advisory ones either)
#   1  blocking findings, advisory findings under --strict, or a --fix run that
#      left the component uncompilable
#   2  the analysis could not be run - a missing tool, a failed cmake configure,
#      a work directory inside the repository, or clang-apply-replacements
#      refusing the recorded fixes (in which case nothing was changed)

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
TIDY_CONFIG="$REPO_ROOT/tests/lint/.clang-tidy"

# Kept out of the source tree; both tools write a lot of scratch.
WORK_DIR="${ONEC_LINT_DIR:-${TMPDIR:-/tmp}/onec-librdkafka-lint}"
CC_DIR="$WORK_DIR/compiledb"
TIDY_DIR="$WORK_DIR/tidy"
SCAN_DIR="$WORK_DIR/scan-build"
SCAN_OUT="$WORK_DIR/scan-report"

DO_FIX=0
STRICT=0
RUN_TIDY=1
RUN_SCAN=1
JOBS="$(nproc 2>/dev/null || echo 2)"

log()  { printf '[lint] %s\n' "$*"; }
warn() { printf '[lint] WARNING: %s\n' "$*" >&2; }
die()  { printf '[lint] ERROR: %s\n' "$*" >&2; exit 2; }
step() {
    printf '\n[lint] =====================================================\n'
    printf '[lint] %s\n' "$*"
    printf '[lint] =====================================================\n\n'
}

# The header comment of this file, from line 2 to the first line that is not a
# comment. Deriving it beats a hard-coded line range, which goes stale the first
# time somebody adds a paragraph.
usage() {
    awk 'NR < 2 { next } /^#/ { sub(/^#[[:space:]]?/, ""); print; next } { exit }' \
        "${BASH_SOURCE[0]}"
}

#----------------------------------------------------------------------------#
# Arguments
#----------------------------------------------------------------------------#

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fix)       DO_FIX=1 ;;
        --strict)    STRICT=1 ;;
        --tidy-only) RUN_SCAN=0 ;;
        --scan-only) RUN_TIDY=0 ;;
        -j)          shift; [[ $# -gt 0 ]] || die "-j needs a number"; JOBS="$1" ;;
        -j*)         JOBS="${1#-j}" ;;
        -h|--help)   usage; exit 0 ;;
        *)           printf '[lint] unknown option: %s\n\n' "$1" >&2
                     usage >&2
                     exit 2 ;;
    esac
    shift
done

[[ "$JOBS" =~ ^[0-9]+$ && "$JOBS" -ge 1 ]] || die "-j expects a positive integer, got '$JOBS'"

if [[ $RUN_TIDY -eq 0 && $DO_FIX -eq 1 ]]; then
    die "--fix applies clang-tidy's fixes, so it cannot be combined with --scan-only"
fi

#----------------------------------------------------------------------------#
# Preconditions
#----------------------------------------------------------------------------#

command -v cmake >/dev/null 2>&1 || die "cmake not found on PATH"
[[ -f "$TIDY_CONFIG" ]] || die "the clang-tidy configuration is missing: $TIDY_CONFIG"

if [[ $RUN_TIDY -eq 1 ]]; then
    command -v clang-tidy >/dev/null 2>&1 \
        || die "clang-tidy not found on PATH (apt install clang-tidy, or use --scan-only)"
fi
if [[ $RUN_SCAN -eq 1 ]]; then
    if ! command -v scan-build >/dev/null 2>&1; then
        warn "scan-build not found on PATH (apt install clang-tools); skipping the static analyzer"
        RUN_SCAN=0
    fi
fi

# The component's CMakeLists.txt writes its .so to ${CMAKE_BINARY_DIR}/../out64.
# With a work directory inside the repository that resolves to the real
# out64/librdkafka_onec.so, and a lint run would silently replace the shipped
# component with an analyzer build. Refuse rather than explain it afterwards.
case "$WORK_DIR" in
    "$REPO_ROOT"|"$REPO_ROOT"/*)
        die "ONEC_LINT_DIR must be outside the repository ($WORK_DIR is inside $REPO_ROOT);
       the analyzer build would overwrite out64/librdkafka_onec.so" ;;
esac

mkdir -p "$WORK_DIR"

#----------------------------------------------------------------------------#
# The file set: src/*.cpp and src/*.h, minus the vendored ones
#----------------------------------------------------------------------------#

# Printed on every run: the reader has to be able to see what was looked at,
# and more importantly what was not.
SOURCES=()
while IFS= read -r f; do
    SOURCES+=("$f")
done < <(find "$REPO_ROOT/src" -maxdepth 1 -name '*.cpp' ! -name 'rdkafka*' | sort)

HEADERS=()
while IFS= read -r f; do
    HEADERS+=("$f")
done < <(find "$REPO_ROOT/src" -maxdepth 1 -name '*.h' ! -name 'rdkafka.h' ! -name 'rdkafkacpp.h' \
         | sort)

[[ ${#SOURCES[@]} -gt 0 ]] || die "no sources found under $REPO_ROOT/src"

#----------------------------------------------------------------------------#
# compile_commands.json
#----------------------------------------------------------------------------#
# clang-tidy needs the real flags (-I include, -DLIBRDKAFKA_STATICLIB=1,
# -std=gnu++17). Guessing them by hand is how a lint script silently starts
# analysing a different program than the one that ships.

step "generating compile_commands.json"

cmake -S "$REPO_ROOT" -B "$CC_DIR" \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DCMAKE_BUILD_TYPE=Release >"$WORK_DIR/cmake-configure.log" 2>&1 \
    || { tail -n 30 "$WORK_DIR/cmake-configure.log" >&2
         die "cmake configure failed; full log in $WORK_DIR/cmake-configure.log"; }

[[ -f "$CC_DIR/compile_commands.json" ]] \
    || die "cmake did not produce $CC_DIR/compile_commands.json"

log "compile database: $CC_DIR/compile_commands.json"

#----------------------------------------------------------------------------#
# clang-tidy
#----------------------------------------------------------------------------#

TIDY_BLOCKING=0
TIDY_ADVISORY=0
TIDY_LOG="$TIDY_DIR/findings.txt"

# Checks that are reported but never auto-fixed.
#
# bugprone-reserved-identifier (and its cert-* aliases) is right that
# __COMPONENT_TYPES_H__ is a reserved name - but its fix rewrites the #define of
# an include guard and leaves the matching #ifndef alone:
#
#   #ifndef __COMPONENT_TYPES_H__      <- untouched
#   #define COMPONENT_TYPES_H_         <- rewritten
#
# The guard then never matches, the header is included twice and the build dies
# with "redefinition of 'delivery_record'". Verified on this tree: --fix without
# this exclusion breaks all ten headers under src/. Renaming the guards is a
# two-minute job by hand and must be done by hand.
FIX_EXCLUDED_CHECKS='-bugprone-reserved-identifier,-cert-dcl37-c,-cert-dcl51-cpp'

# Headers are analysed through the .cpp files that include them
# (HeaderFilterRegex in tests/lint/.clang-tidy decides which), which is the only
# way a header can be given the right compile flags. They are listed here so the
# run says out loud that they were covered.
run_clang_tidy() {
    rm -rf "$TIDY_DIR"
    mkdir -p "$TIDY_DIR/raw" "$TIDY_DIR/fixes"

    # --fix is NOT clang-tidy's own --fix. Eighteen translation units include the
    # same ten headers, and --fix rewrites a header as soon as the first of them
    # reaches it; the next seventeen then compute their replacements against text
    # that has already moved, and the headers end up mangled. That is not a
    # theory - it is what happened here before this was changed.
    #
    # The supported way is the one run-clang-tidy uses: every process only
    # RECORDS its replacements as YAML, and clang-apply-replacements merges the
    # lot afterwards, deduplicating the ones that several units found in the same
    # header and refusing the ones that conflict. Nothing on disk changes until
    # the analysis is over.
    local fix_args=()
    if [[ $DO_FIX -eq 1 ]]; then
        fix_args=(--checks="$FIX_EXCLUDED_CHECKS")
        log "--fix: files under src/ WILL be modified once the analysis finishes."
        log "       Commit or stash first; the build is re-run afterwards to prove"
        log "       the fixes did not break anything."
    fi

    # One process per file, for three reasons: clang-tidy aborts on this file set
    # when several translation units are passed to a single invocation
    # (reproducible with clang 18 on this tree), a crash in one file must not
    # lose the results for the other seventeen, and it parallelises for free.
    log "running clang-tidy over ${#SOURCES[@]} source file(s), ${JOBS} at a time"

    local running=0
    local src base
    for src in "${SOURCES[@]}"; do
        base="$(basename "$src")"
        (
            local_fix=()
            if [[ $DO_FIX -eq 1 ]]; then
                local_fix=(--export-fixes="$TIDY_DIR/fixes/$base.yaml")
            fi
            clang-tidy \
                -p "$CC_DIR" \
                --config-file="$TIDY_CONFIG" \
                --quiet \
                "${fix_args[@]}" \
                "${local_fix[@]}" \
                "$src" \
                >"$TIDY_DIR/raw/$base.out" 2>"$TIDY_DIR/raw/$base.err" || true
        ) &
        running=$((running + 1))
        if [[ $running -ge $JOBS ]]; then
            wait -n 2>/dev/null || true
            running=$((running - 1))
        fi
    done
    wait

    # A crash leaves an empty .out and a stack trace in .err; that is a tooling
    # failure, not a clean file, and it must not be reported as "no findings".
    local crashed=0
    for src in "${SOURCES[@]}"; do
        base="$(basename "$src")"
        if grep -q "PLEASE submit a bug report\|Stack dump" "$TIDY_DIR/raw/$base.err" 2>/dev/null; then
            warn "clang-tidy crashed on $base - see $TIDY_DIR/raw/$base.err"
            crashed=$((crashed + 1))
        fi
    done
    [[ $crashed -eq 0 ]] || warn "$crashed file(s) were not fully analysed"

    # Deduplicate. A header diagnostic is re-reported by every .cpp that
    # includes the header, so the raw count over-states the work by a factor of
    # five or more; the key is file:line:col:check.
    cat "$TIDY_DIR"/raw/*.out 2>/dev/null \
        | grep -E '^[^ ]+:[0-9]+:[0-9]+: (warning|error):' \
        | sed "s|^$REPO_ROOT/||" \
        | sort -u > "$TIDY_LOG" || true

    TIDY_BLOCKING="$(grep -cE ': error:|\[clang-analyzer-' "$TIDY_LOG" || true)"
    local total
    total="$(grep -c . "$TIDY_LOG" || true)"
    TIDY_ADVISORY=$((total - TIDY_BLOCKING))

    if [[ $total -gt 0 ]]; then
        printf '\n[lint] findings by check:\n'
        grep -oE '\[[a-zA-Z0-9.,_-]+\]$' "$TIDY_LOG" \
            | sort | uniq -c | sort -rn | sed 's/^/[lint]   /'
    fi

    if [[ "$TIDY_BLOCKING" -gt 0 ]]; then
        printf '\n[lint] BLOCKING clang-tidy findings:\n'
        grep -E ': error:|\[clang-analyzer-' "$TIDY_LOG" | sed 's/^/[lint]   /'
    fi

    log ""
    log "clang-tidy: $TIDY_BLOCKING blocking, $TIDY_ADVISORY advisory (full list: $TIDY_LOG)"

    if [[ $DO_FIX -eq 1 ]]; then
        apply_fixes
    fi
}

#----------------------------------------------------------------------------#
# Applying the recorded fixes, and proving they did no harm
#----------------------------------------------------------------------------#

apply_fixes() {
    local applier=""
    local candidate
    for candidate in clang-apply-replacements clang-apply-replacements-18 \
                     clang-apply-replacements-17 clang-apply-replacements-16; do
        if command -v "$candidate" >/dev/null 2>&1; then
            applier="$candidate"
            break
        fi
    done
    [[ -n "$applier" ]] \
        || die "--fix needs clang-apply-replacements (apt install clang-tools); nothing was changed"

    # No YAML at all means clang-tidy had no automatic fix to offer. Say so
    # rather than reporting a successful fix run that changed nothing.
    if ! find "$TIDY_DIR/fixes" -name '*.yaml' -size +0 | grep -q .; then
        log "no automatic fixes were available; src/ was not touched"
        return
    fi

    step "applying fixes"
    # No --format: that would reformat every line a fix touches with clang-format,
    # and this repository has no .clang-format, so the result would be a diff of
    # mostly-unrelated whitespace. Off is the default; it is not passed at all
    # because --format is a boolean flag whose spelling has changed between
    # releases.
    log "merging the recorded replacements with $applier"
    "$applier" "$TIDY_DIR/fixes" \
        || die "clang-apply-replacements failed; check src/ with 'git diff' before continuing"

    # A --fix that leaves the component uncompilable is worse than no --fix at
    # all, and the failure has to be found here rather than by the next person
    # to run a build. This is the same cmake tree the compile database came
    # from, so it costs one compile of src/ and nothing else.
    step "rebuilding to verify the fixes"
    if cmake --build "$CC_DIR" -j "$JOBS" >"$WORK_DIR/post-fix-build.log" 2>&1; then
        log "the component still builds after --fix"
    else
        tail -n 30 "$WORK_DIR/post-fix-build.log" >&2
        printf '\n[lint] ERROR: the component NO LONGER BUILDS after --fix.\n' >&2
        printf '[lint] Undo it with:  git -C %s checkout -- src/\n' "$REPO_ROOT" >&2
        printf '[lint] Full log: %s\n' "$WORK_DIR/post-fix-build.log" >&2
        exit 1
    fi
}

#----------------------------------------------------------------------------#
# scan-build
#----------------------------------------------------------------------------#

SCAN_BUGS=0

run_scan_build() {
    rm -rf "$SCAN_DIR" "$SCAN_OUT"
    mkdir -p "$SCAN_OUT"

    # scan-build has to drive the compiler itself, so it gets its own build
    # tree configured with its interposing clang. The component links librdkafka
    # statically from lib/linux64; that part is untouched.
    #
    # --exclude keeps the vendored headers out of the report. It takes a
    # directory or a file and drops any bug whose primary location is inside it.
    local scan_common=(
        --status-bugs
        -o "$SCAN_OUT"
        --exclude "$REPO_ROOT/src/nlohmann"
        --exclude "$REPO_ROOT/include"
        -disable-checker deadcode.DeadStores
    )

    log "configuring the scan-build tree in $SCAN_DIR"
    if ! scan-build "${scan_common[@]}" \
            cmake -S "$REPO_ROOT" -B "$SCAN_DIR" -DCMAKE_BUILD_TYPE=Debug \
            >"$WORK_DIR/scan-configure.log" 2>&1; then
        tail -n 30 "$WORK_DIR/scan-configure.log" >&2
        die "scan-build could not configure the component; log in $WORK_DIR/scan-configure.log"
    fi

    log "analysing (this compiles the whole component, it takes a while)"
    local rc=0
    scan-build "${scan_common[@]}" \
        cmake --build "$SCAN_DIR" -j "$JOBS" \
        >"$WORK_DIR/scan-build.log" 2>&1 || rc=$?

    # --status-bugs makes scan-build exit 1 when it found something. Anything
    # else is a build failure, which must not be reported as a clean analysis.
    if [[ $rc -ne 0 && $rc -ne 1 ]]; then
        tail -n 40 "$WORK_DIR/scan-build.log" >&2
        die "the scan-build compilation failed (exit $rc); log in $WORK_DIR/scan-build.log"
    fi

    # The report directory holds one HTML file per bug plus an index.
    SCAN_BUGS="$(find "$SCAN_OUT" -name 'report-*.html' 2>/dev/null | grep -c . || true)"

    if [[ "$SCAN_BUGS" -gt 0 ]]; then
        printf '\n[lint] scan-build reports:\n'
        grep -hoE '<!-- BUG(FILE|LINE|DESC|TYPE) .* -->' "$SCAN_OUT"/*/report-*.html 2>/dev/null \
            | sed 's/<!-- //; s/ -->$//' | sed 's/^/[lint]   /' || true
        local index
        index="$(find "$SCAN_OUT" -name 'index.html' | head -n 1)"
        if [[ -n "$index" ]]; then
            log "full report: $index"
        fi
    fi

    log ""
    log "scan-build: $SCAN_BUGS report(s)"
}

#----------------------------------------------------------------------------#
# Run
#----------------------------------------------------------------------------#

MODE_TEXT="report only"
if [[ $DO_FIX -eq 1 ]]; then
    MODE_TEXT="FIX (files under src/ will be modified)"
fi
GATE_TEXT="blocking findings only"
if [[ $STRICT -eq 1 ]]; then
    GATE_TEXT="strict - advisory findings also fail the run"
fi

step "onec-librdkafka static analysis"
log "repository : $REPO_ROOT"
log "config     : $TIDY_CONFIG"
log "work dir   : $WORK_DIR"
log "sources    : ${#SOURCES[@]} file(s) in src/ (vendored rdkafka*.h and nlohmann/ excluded)"
log "headers    : ${#HEADERS[@]} own header(s), analysed through their includers"
log "mode       : $MODE_TEXT"
log "gate       : $GATE_TEXT"

if [[ $RUN_TIDY -eq 1 ]]; then
    step "clang-tidy"
    run_clang_tidy
fi

if [[ $RUN_SCAN -eq 1 ]]; then
    step "scan-build (Clang Static Analyzer)"
    run_scan_build
fi

#----------------------------------------------------------------------------#
# Verdict
#----------------------------------------------------------------------------#

BLOCKING=$((TIDY_BLOCKING + SCAN_BUGS))

printf '\n[lint] =====================================================\n'
printf '[lint] SUMMARY\n'
printf '[lint] =====================================================\n'
if [[ $RUN_TIDY -eq 1 ]]; then
    printf '[lint]   %-28s %s\n' "clang-tidy blocking" "$TIDY_BLOCKING"
    printf '[lint]   %-28s %s\n' "clang-tidy advisory" "$TIDY_ADVISORY"
else
    printf '[lint]   %-28s %s\n' "clang-tidy" "not run (--scan-only)"
fi
if [[ $RUN_SCAN -eq 1 ]]; then
    printf '[lint]   %-28s %s\n' "scan-build reports" "$SCAN_BUGS"
else
    printf '[lint]   %-28s %s\n' "scan-build" "not run"
fi
printf '[lint] =====================================================\n'

if [[ $DO_FIX -eq 1 ]]; then
    printf '\n[lint] --fix was applied. Review the diff before committing:\n'
    printf '[lint]     git -C %s diff -- src/\n' "$REPO_ROOT"
    printf '[lint] Then rebuild and re-run the tests: tests/run-tests.sh\n'
fi

if [[ $BLOCKING -gt 0 ]]; then
    printf '\n[lint] FAILED: %d blocking finding(s).\n' "$BLOCKING" >&2
    printf '[lint] These are path-sensitive analyzer results or hard errors; they are\n' >&2
    printf '[lint] not style opinions and should not be waived without a reason in the\n' >&2
    printf '[lint] commit message.\n' >&2
    exit 1
fi

if [[ $STRICT -eq 1 && $TIDY_ADVISORY -gt 0 ]]; then
    printf '\n[lint] FAILED (--strict): %d advisory finding(s).\n' "$TIDY_ADVISORY" >&2
    printf '[lint] Full list: %s\n' "$TIDY_LOG" >&2
    exit 1
fi

if [[ $TIDY_ADVISORY -gt 0 ]]; then
    printf '\n[lint] no blocking findings. %d advisory finding(s) remain - see %s\n' \
        "$TIDY_ADVISORY" "$TIDY_LOG"
else
    printf '\n[lint] clean.\n'
fi
exit 0
