#!/usr/bin/env bash
#
# One command for the whole onec-librdkafka test suite:
#
#   build the tests -> smoke test -> Kafka 4.x broker up -> TLS test -> down
#
# The broker is always brought down again, including when the test fails or the
# run is interrupted (trap EXIT). --keep-up suppresses that for debugging.
#
# Exit codes:
#   0   everything that was supposed to run passed, and TLS really was tested
#   1   something failed
#   2   bad command line
#   3   the run finished without testing TLS at all (--no-docker). Nothing
#       broke, but nothing was proven either - pass --allow-smoke-only to turn
#       that into a 0 on purpose.
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"
BUILD_DIR="${ONEC_TESTS_BUILD_DIR:-$SCRIPT_DIR/build}"
SO_PATH="${ONEC_KAFKA_SO:-$REPO_ROOT/out64/librdkafka_onec.so}"

# up.sh writes the version the broker actually reported here.
BROKER_VERSION_FILE="$DOCKER_DIR/secrets/broker-version"

USE_DOCKER=1
RUN_SSL=1
RUN_SOAK=0
SOAK_MESSAGES=""
KEEP_UP=0
BROKER_UP=0
SANITIZER="none"
USE_VALGRIND=0
ALLOW_SMOKE_ONLY=0

SMOKE_RESULT="not run"
SSL_RESULT="not run"
SOAK_RESULT="not run"
BROKER_VERSION="not started"

log()  { printf '[run-tests] %s\n' "$*"; }
# Also called from cleanup(), which shellcheck believes to be dead code.
# shellcheck disable=SC2329
warn() { printf '[run-tests] WARNING: %s\n' "$*" >&2; }
step() {
    printf '\n[run-tests] =====================================================\n'
    printf '[run-tests] %s\n' "$*"
    printf '[run-tests] =====================================================\n\n'
}
die() { printf '\n[run-tests] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'USAGE'
Usage: run-tests.sh [options]

  (no options)        build, smoke test, broker up, TLS test, broker down
  --no-docker         smoke test only: no broker, no TLS test. Exits 3, because
                      a run that never touched TLS is not a green run.
  --allow-smoke-only  ...unless you say so explicitly: makes --no-docker exit 0
  --external-broker   do not manage docker, but do run the TLS test against
                      $KAFKA_SSL_BOOTSTRAP (which must be set)
  --keep-up           leave the broker running when the run finishes
  --soak [N]          also run the soak/leak test (tests/soak_test.cpp) with N
                      messages; without N it uses its own default, which is a
                      million. It needs a broker, so combine it with the
                      default mode or with --external-broker.
  --valgrind          run the smoke test under valgrind memcheck
                      (--leak-check=full --error-exitcode=1) instead of bare
  --asan              rebuild with -fsanitize=address,undefined and run
  --tsan              rebuild with -fsanitize=thread and run
  -h, --help          this text

  --valgrind cannot be combined with --asan/--tsan.
  --asan/--tsan build into a separate tree ("<build dir>-asan" / "-tsan") so
  the sanitized and plain builds do not keep invalidating each other's cache.

Environment:
  ONEC_KAFKA_SO         component under test
                        (default: <repo>/out64/librdkafka_onec.so)
  ONEC_TESTS_BUILD_DIR  cmake build tree (default: tests/build)
  KAFKA_SSL_BOOTSTRAP   broker address  (default: localhost:<port from docker/.env>)
  KAFKA_SSL_CA          CA bundle       (default: tests/docker/secrets/ca.pem)
  KAFKA_TEST_OTHER_CA   unrelated CA used as the negative control
                        (default: tests/docker/secrets/other-ca.pem)
  KAFKA_TEST_TOPIC      topic           (default: the one docker/.env creates)
  KAFKA_TEST_EXPECT_BROKER_MAJOR
                        broker generation the suite insists on (default: 4)
  KAFKA_TEST_BROKER_VERSION
                        the version the broker reported. Filled in from
                        docker/secrets/broker-version, which up.sh writes after
                        querying the running container.

  Every KAFKA_TEST_* variable the TLS test understands is passed straight
  through - see tests/README.md. Relative paths in these variables are
  resolved from the repository root.
USAGE
}

#----------------------------------------------------------------------------#
# Arguments
#----------------------------------------------------------------------------#

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-docker)        USE_DOCKER=0; RUN_SSL=0 ;;
        --allow-smoke-only) ALLOW_SMOKE_ONLY=1 ;;
        --external-broker)  USE_DOCKER=0; RUN_SSL=1 ;;
        --keep-up)          KEEP_UP=1 ;;
        --soak)
            RUN_SOAK=1
            # The count is optional: "--soak" alone lets the soak test use its
            # own default.
            if [[ "${2-}" =~ ^[0-9]+$ ]]; then
                SOAK_MESSAGES="$2"
                shift
            fi
            ;;
        --valgrind)         USE_VALGRIND=1 ;;
        --asan)             SANITIZER="address" ;;
        --tsan)             SANITIZER="thread" ;;
        -h|--help)          usage; exit 0 ;;
        *)                  printf 'unknown option: %s\n\n' "$1" >&2
                            usage >&2
                            exit 2 ;;
    esac
    shift
done

if [[ $USE_VALGRIND -eq 1 && "$SANITIZER" != "none" ]]; then
    printf 'valgrind and the sanitizers cannot be used together\n\n' >&2
    usage >&2
    exit 2
fi

# Absolute before anything changes directory, so a relative ONEC_KAFKA_SO or
# ONEC_TESTS_BUILD_DIR still means what the caller meant.
[[ "$SO_PATH"   = /* ]] || SO_PATH="$PWD/$SO_PATH"
[[ "$BUILD_DIR" = /* ]] || BUILD_DIR="$PWD/$BUILD_DIR"

# A sanitized build is a different build. Keeping it in its own tree means
# alternating between `run-tests.sh` and `run-tests.sh --asan` does not force a
# full rebuild every time.
case "$SANITIZER" in
    address) BUILD_DIR="${BUILD_DIR%/}-asan" ;;
    thread)  BUILD_DIR="${BUILD_DIR%/}-tsan" ;;
    none)    ;;
esac

# The test binaries look for tests/docker/secrets/ca.pem relative to the working
# directory, exactly as ctest runs them.
cd "$REPO_ROOT"

#----------------------------------------------------------------------------#
# Teardown. Registered before anything can start a container.
#----------------------------------------------------------------------------#

# Invoked by `trap cleanup EXIT` right below; shellcheck does not follow traps.
# shellcheck disable=SC2329
cleanup() {
    local rc=$?
    if [[ $BROKER_UP -eq 1 ]]; then
        if [[ $KEEP_UP -eq 1 ]]; then
            printf '\n[run-tests] --keep-up: the broker is still running.\n'
            printf '[run-tests]   stop it with %s/down.sh\n' "$DOCKER_DIR"
        else
            printf '\n[run-tests] stopping the Kafka test broker\n'
            "$DOCKER_DIR/down.sh" >/dev/null 2>&1 \
                || warn "down.sh failed - check 'docker ps' for leftovers"
        fi
    fi
    exit "$rc"
}
trap cleanup EXIT

#----------------------------------------------------------------------------#
# Helpers
#----------------------------------------------------------------------------#

# Reads one KEY=VALUE out of tests/docker/.env without sourcing it: sourcing
# would silently overwrite variables the caller already exported (the
# KAFKA_TEST_* family in particular).
read_env() {
    local key="$1"
    [[ -f "$DOCKER_DIR/.env" ]] || return 0
    sed -n "s/^${key}=//p" "$DOCKER_DIR/.env" | tail -n 1
}

# Turns a test's exit code into a verdict: echoes the text, returns 0 only when
# the run may still be called successful.
test_verdict() {
    case "$1" in
        0)  echo "PASS"; return 0 ;;
        1)  echo "FAIL (checks failed)"; return 1 ;;
        2)  echo "FAIL (component could not be loaded)"; return 1 ;;
        3)  echo "FAIL (hard timeout, killed by the watchdog)"; return 1 ;;
        77) echo "SKIP (broker unreachable - nothing was tested)"; return 1 ;;
        *)  echo "FAIL (unexpected exit code $1)"; return 1 ;;
    esac
}

#----------------------------------------------------------------------------#
# 0. Preconditions
#----------------------------------------------------------------------------#

command -v cmake >/dev/null 2>&1 || die "cmake not found on PATH"

if [[ $USE_VALGRIND -eq 1 ]]; then
    command -v valgrind >/dev/null 2>&1 \
        || die "--valgrind given but valgrind is not on PATH"
fi

if [[ ! -f "$SO_PATH" ]]; then
    {
        printf '\n[run-tests] ERROR: the component is not built.\n'
        printf '[run-tests]\n'
        printf '[run-tests]   expected here : %s\n' "$SO_PATH"
        printf '[run-tests]\n'
        printf '[run-tests]   Build it first:\n'
        printf '[run-tests]       %s/scripts/build-component-linux.sh\n' "$REPO_ROOT"
        printf '[run-tests]\n'
        printf '[run-tests]   That needs lib/linux64/librdkafka*.a; if those are missing, run\n'
        printf '[run-tests]       %s/scripts/build-librdkafka-linux.sh\n' "$REPO_ROOT"
        printf '[run-tests]   first (it takes a while).\n'
        printf '[run-tests]\n'
        printf '[run-tests]   Or point the suite at an existing build:\n'
        printf '[run-tests]       ONEC_KAFKA_SO=/path/to/librdkafka_onec.so %s\n' "$0"
    } >&2
    exit 1
fi

if [[ $RUN_SSL -eq 1 && $USE_DOCKER -eq 0 && -z "${KAFKA_SSL_BOOTSTRAP:-}" ]]; then
    die "--external-broker needs KAFKA_SSL_BOOTSTRAP (host:port of the SSL listener)"
fi

if [[ $RUN_SOAK -eq 1 && $RUN_SSL -eq 0 ]]; then
    die "--soak needs a broker: drop --no-docker, or use --external-broker"
fi

#----------------------------------------------------------------------------#
# 1. Build
#----------------------------------------------------------------------------#

step "building the tests in $BUILD_DIR"

cmake_args=(
    -DONEC_KAFKA_SO="$SO_PATH"
    -DONEC_TESTS_SANITIZE="$SANITIZER"
)

# Whatever this script decides, a later hand-run `ctest` in the same tree should
# decide the same way: no silent skips when we brought a broker up ourselves,
# and the soak test selectable by label when it was asked for.
if [[ $RUN_SSL -eq 1 ]]; then
    cmake_args+=(-DONEC_TESTS_REQUIRE_BROKER=ON)
else
    cmake_args+=(-DONEC_TESTS_REQUIRE_BROKER=OFF)
fi
if [[ $RUN_SOAK -eq 1 ]]; then
    cmake_args+=(-DONEC_TESTS_SOAK=ON)
fi

cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" "${cmake_args[@]}" \
    || die "cmake configure failed"

jobs_n="$(nproc 2>/dev/null || echo 2)"
cmake --build "$BUILD_DIR" -j "$jobs_n" \
    || die "cmake build failed"

#----------------------------------------------------------------------------#
# 2. Smoke test - no broker, no network
#----------------------------------------------------------------------------#

smoke_cmd=("$BUILD_DIR/smoke_test" "$SO_PATH")
smoke_what="smoke test: the component loads and its 1C surface is intact"

if [[ $USE_VALGRIND -eq 1 ]]; then
    smoke_what="$smoke_what (under valgrind memcheck)"
    # --errors-for-leak-kinds=definite: librdkafka keeps a good deal of
    # still-reachable global state alive on purpose, and the component is
    # dlopen()ed, so only a definite loss is evidence of a bug here.
    smoke_cmd=(
        valgrind
        "--tool=memcheck"
        "--leak-check=full"
        "--show-leak-kinds=definite,indirect"
        "--errors-for-leak-kinds=definite,indirect"
        "--track-origins=yes"
        "--error-exitcode=1"
        "${smoke_cmd[@]}"
    )
fi

step "$smoke_what"

smoke_rc=0
"${smoke_cmd[@]}" || smoke_rc=$?

if [[ $smoke_rc -eq 0 ]]; then
    if [[ $USE_VALGRIND -eq 1 ]]; then
        SMOKE_RESULT="PASS (valgrind clean)"
    else
        SMOKE_RESULT="PASS"
    fi
else
    if [[ $USE_VALGRIND -eq 1 ]]; then
        SMOKE_RESULT="FAIL (exit $smoke_rc, under valgrind)"
    else
        SMOKE_RESULT="FAIL (exit $smoke_rc)"
    fi

    log ""
    log "the smoke test failed, so the component itself is broken."
    log "not starting the broker - fix this first."
    log "(to run the TLS test regardless: $BUILD_DIR/kafka_ssl_test)"

    printf '\n[run-tests] SUMMARY\n'
    printf '[run-tests]   %-12s %s\n' "smoke"     "$SMOKE_RESULT"
    printf '[run-tests]   %-12s %s\n' "kafka-ssl" "not run (smoke failed)"
    printf '[run-tests]   %-12s %s\n' "soak"      "not run (smoke failed)"
    exit 1
fi

#----------------------------------------------------------------------------#
# 3. The broker
#----------------------------------------------------------------------------#

if [[ $USE_DOCKER -eq 1 ]]; then
    step "bringing the Kafka 4.x test broker up"

    command -v docker >/dev/null 2>&1 \
        || die "docker not found on PATH (use --no-docker to run the smoke test only)"

    # Set the flag first: a partially started stack must still be torn down.
    BROKER_UP=1
    "$DOCKER_DIR/up.sh" || die "tests/docker/up.sh failed - see the broker log above"

    ssl_port="$(read_env KAFKA_SSL_PORT)"
    [[ -n "$ssl_port" ]] || ssl_port=9093

    export KAFKA_SSL_BOOTSTRAP="${KAFKA_SSL_BOOTSTRAP:-localhost:$ssl_port}"
    export KAFKA_SSL_CA="${KAFKA_SSL_CA:-$DOCKER_DIR/secrets/ca.pem}"

    # The topic the init step actually created. Reading it here is what keeps
    # compose and the test binary on the same name; when they drifted apart the
    # suite only passed because the broker auto-creates topics, which meant the
    # init step proved nothing.
    env_topic="$(read_env KAFKA_TEST_TOPIC)"
    if [[ -n "$env_topic" ]]; then
        if [[ -n "${KAFKA_TEST_TOPIC:-}" && "$KAFKA_TEST_TOPIC" != "$env_topic" ]]; then
            warn "KAFKA_TEST_TOPIC=$KAFKA_TEST_TOPIC overrides docker/.env ($env_topic)," \
                 "so the topic the init step created is not the one under test"
        fi
        export KAFKA_TEST_TOPIC="${KAFKA_TEST_TOPIC:-$env_topic}"
    fi

    # up.sh asked the container for its version; use that, never the image tag.
    if [[ -z "${KAFKA_TEST_BROKER_VERSION:-}" ]]; then
        if [[ -s "$BROKER_VERSION_FILE" ]]; then
            KAFKA_TEST_BROKER_VERSION="$(tr -d '\r\n' <"$BROKER_VERSION_FILE")"
            export KAFKA_TEST_BROKER_VERSION
        else
            warn "up.sh left no $BROKER_VERSION_FILE - the broker version cannot be forwarded"
        fi
    fi
fi

if [[ $RUN_SSL -eq 1 ]]; then
    # The negative control for certificate verification. gen-certs.sh mints it
    # next to ca.pem; it signs nothing, so pointing a client at it must fail.
    export KAFKA_TEST_OTHER_CA="${KAFKA_TEST_OTHER_CA:-$DOCKER_DIR/secrets/other-ca.pem}"

    # Which broker generation this suite is allowed to call a pass.
    export KAFKA_TEST_EXPECT_BROKER_MAJOR="${KAFKA_TEST_EXPECT_BROKER_MAJOR:-4}"

    # This run went to the trouble of providing a broker, so the test skipping
    # for the lack of one is a failure of the run, not a neutral outcome.
    export KAFKA_TEST_REQUIRE_BROKER="${KAFKA_TEST_REQUIRE_BROKER:-1}"

    if [[ -n "${KAFKA_TEST_BROKER_VERSION:-}" ]]; then
        BROKER_VERSION="$KAFKA_TEST_BROKER_VERSION"
    else
        BROKER_VERSION="unknown"
        warn "the broker version is unknown - it cannot be checked against" \
             "KAFKA_TEST_EXPECT_BROKER_MAJOR=$KAFKA_TEST_EXPECT_BROKER_MAJOR"
    fi
fi

#----------------------------------------------------------------------------#
# 4. The TLS test
#----------------------------------------------------------------------------#

ssl_ok=0
soak_ok=0

if [[ $RUN_SSL -eq 1 ]]; then
    step "TLS test: the component talks SSL to the broker (Kafka $BROKER_VERSION)"

    ssl_rc=0
    "$BUILD_DIR/kafka_ssl_test" "$SO_PATH" || ssl_rc=$?

    # The text is produced either way; the status says whether it is a pass.
    SSL_RESULT="$(test_verdict "$ssl_rc")" || ssl_ok=1
else
    SSL_RESULT="not run (--no-docker)"
fi

#----------------------------------------------------------------------------#
# 5. The soak / leak test - only when asked for
#----------------------------------------------------------------------------#

if [[ $RUN_SOAK -eq 1 ]]; then
    if [[ $ssl_ok -ne 0 ]]; then
        SOAK_RESULT="not run (the TLS test did not pass)"
    else
        step "soak test: ${SOAK_MESSAGES:-its default number of} messages"

        # --messages=N is soak_test's own option and it beats the environment,
        # so nothing the caller exported can quietly change the count.
        soak_cmd=("$BUILD_DIR/soak_test" "$SO_PATH")
        if [[ -n "$SOAK_MESSAGES" ]]; then
            soak_cmd+=("--messages=$SOAK_MESSAGES")
        fi

        soak_rc=0
        "${soak_cmd[@]}" || soak_rc=$?

        SOAK_RESULT="$(test_verdict "$soak_rc")" || soak_ok=1
    fi
fi

#----------------------------------------------------------------------------#
# 6. Summary
#----------------------------------------------------------------------------#

sanitize_line="$SANITIZER"
if [[ $USE_VALGRIND -eq 1 ]]; then
    sanitize_line="none (smoke ran under valgrind)"
fi

printf '\n[run-tests] =====================================================\n'
printf '[run-tests] SUMMARY\n'
printf '[run-tests] =====================================================\n'
printf '[run-tests]   component    %s\n' "$SO_PATH"
printf '[run-tests]   build        %s\n' "$BUILD_DIR"
printf '[run-tests]   sanitizer    %s\n' "$sanitize_line"
printf '[run-tests]   broker       %s\n' "$BROKER_VERSION"
printf '[run-tests]   %-12s %s\n' "smoke"     "$SMOKE_RESULT"
printf '[run-tests]   %-12s %s\n' "kafka-ssl" "$SSL_RESULT"
printf '[run-tests]   %-12s %s\n' "soak"      "$SOAK_RESULT"
printf '[run-tests] =====================================================\n'

if [[ $ssl_ok -ne 0 ]]; then
    {
        printf '\n[run-tests] the TLS test did not pass.\n'
        if [[ "$SSL_RESULT" == SKIP* ]]; then
            printf '[run-tests] a skip here means the test could not reach the broker this run\n'
            printf '[run-tests] was supposed to provide - that is a failure of the run, not a pass.\n'
        fi
        printf '[run-tests] see the troubleshooting section of tests/README.md.\n'
    } >&2
    exit 1
fi

if [[ $soak_ok -ne 0 ]]; then
    printf '\n[run-tests] the soak test did not pass.\n' >&2
    exit 1
fi

# Nothing failed - but "nothing failed" is not the same claim as "TLS works".
if [[ $RUN_SSL -eq 0 ]]; then
    if [[ $ALLOW_SMOKE_ONLY -eq 1 ]]; then
        printf '\n[run-tests] SMOKE ONLY - TLS was not tested (--allow-smoke-only).\n'
        printf '[run-tests] the smoke test passed; nothing was proven about SSL.\n'
        exit 0
    fi
    {
        printf '\n[run-tests] SMOKE ONLY - TLS was not tested.\n'
        printf '[run-tests]\n'
        printf '[run-tests] The smoke test passed, and that is all that happened: no broker\n'
        printf '[run-tests] was started and not one byte of TLS was exercised. The whole\n'
        printf '[run-tests] point of this suite is the SSL path against Kafka 4.x, so this\n'
        printf '[run-tests] run does not get to exit 0.\n'
        printf '[run-tests]\n'
        printf '[run-tests]   full run          : %s\n' "$0"
        printf '[run-tests]   own broker        : %s --external-broker  (KAFKA_SSL_BOOTSTRAP=...)\n' "$0"
        printf '[run-tests]   really only smoke : %s --no-docker --allow-smoke-only\n' "$0"
    } >&2
    exit 3
fi

printf '\n[run-tests] all good.\n'
exit 0
