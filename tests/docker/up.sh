#!/usr/bin/env bash
#
# Brings up the Kafka 4.x test broker and blocks until it is genuinely usable:
# the broker answers a Kafka API request, the SSL listener completes a verified
# TLS handshake, and the test topic exists.
#
# "4.x" is asserted, not assumed: once the broker is healthy it is asked for its
# real version and the run dies unless the major version matches
# KAFKA_TEST_EXPECT_BROKER_MAJOR (default 4). The version is exported as
# KAFKA_TEST_BROKER_VERSION and also written to secrets/broker-version, which is
# how run-tests.sh forwards it to the test binary.
#
# On failure it dumps the tail of the broker log instead of leaving you to
# guess, and exits non-zero.
#
# Usage:
#   ./up.sh                  start (regenerates certs only if needed)
#   ./up.sh --recreate       tear the cluster down first, then start clean
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
. "$SCRIPT_DIR/_common.sh"

COMPOSE_FILE="$SCRIPT_DIR/compose.yml"
KAFKA_CONTAINER="onec-librdkafka-test-kafka"
INIT_CONTAINER="onec-librdkafka-test-init"
CA_PEM="$SCRIPT_DIR/secrets/ca.pem"
OTHER_CA_PEM="$SCRIPT_DIR/secrets/other-ca.pem"

# Where the version this broker actually reports is left for run-tests.sh.
# Inside secrets/, which is gitignored as a whole.
BROKER_VERSION_FILE="$SCRIPT_DIR/secrets/broker-version"

# Which broker generation this rig is allowed to test. The whole point of the
# 2.3.0 -> 2.15.1 librdkafka update was Kafka 4.x, so a 3.x broker passing the
# suite would prove nothing.
EXPECT_MAJOR="${KAFKA_TEST_EXPECT_BROKER_MAJOR:-4}"

# How long to wait for the broker to report itself healthy.
READY_TIMEOUT="${READY_TIMEOUT:-180}"

log()  { printf '[up] %s\n' "$*"; }
fail() { printf '[up] ERROR: %s\n' "$*" >&2; }

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

dump_logs() {
    local out
    printf '\n[up] ---- last 80 lines of the broker log ---------------------\n' >&2
    out="$(compose logs --no-color --tail 80 kafka 2>&1 || true)"
    if [[ -n "$out" ]]; then
        printf '%s\n' "$out" | sed 's/^/[up] /' >&2
    else
        printf '[up] (no broker log - the container never got far enough to produce one)\n' >&2
    fi
    if docker container inspect "$INIT_CONTAINER" >/dev/null 2>&1; then
        printf '[up] ---- init step log ---------------------------------------\n' >&2
        out="$(docker logs --tail 40 "$INIT_CONTAINER" 2>&1 || true)"
        if [[ -n "$out" ]]; then
            printf '%s\n' "$out" | sed 's/^/[up] /' >&2
        fi
    fi
    printf '[up] -----------------------------------------------------------\n\n' >&2
}

# A port already taken by something else produces a confusing docker error, so
# say plainly what is in the way. Skipped when our own broker holds the port.
check_ports_free() {
    _have ss || return 0
    local port
    for port in "$KAFKA_PLAINTEXT_PORT" "$KAFKA_SSL_PORT"; do
        if ss -ltn "sport = :${port}" 2>/dev/null | grep -q LISTEN; then
            fail "port ${port} is already in use by another process"
            ss -ltnp "sport = :${port}" 2>/dev/null | sed 's/^/[up]   /' >&2 || true
            exit 1
        fi
    done
}

die() { fail "$*"; dump_logs; exit 1; }

case "${1-}" in
    --recreate) log "--recreate: tearing the existing cluster down first"
                compose down -v --remove-orphans >/dev/null 2>&1 || true ;;
    "")         ;;
    *)          echo "usage: $(basename "$0") [--recreate]" >&2; exit 2 ;;
esac

_have docker || { fail "docker not found on PATH"; exit 1; }
docker compose version >/dev/null 2>&1 || { fail "docker compose v2 not available"; exit 1; }

# ---------------------------------------------------------------------------
# 1. Certificates. gen-certs.sh is idempotent - it validates what is there and
#    no-ops when the material is already consistent.
# ---------------------------------------------------------------------------
log "checking TLS material"
"$SCRIPT_DIR/gen-certs.sh" || { fail "certificate generation failed"; exit 1; }

# A broker reads its keystore once, at startup, so a CA rotated underneath a
# running broker leaves it serving a certificate no client can verify any more.
# Asking the live listener what it actually serves catches that regardless of
# whether the rotation happened during this run or in an earlier one.
broker_serves_current_ca() {
    local out
    WORK_DIR="$SCRIPT_DIR/secrets"
    out="$(run_openssl_hostnet s_client -connect "localhost:${KAFKA_SSL_PORT}" \
             -CAfile ca.pem -verify_return_error -brief </dev/null 2>&1)" || return 1
    [[ "$out" == *"Verification: OK"* ]]
}

recreate=()
if [[ "$(docker inspect -f '{{.State.Running}}' "$KAFKA_CONTAINER" 2>/dev/null)" != "true" ]]; then
    check_ports_free
else
    health="$(docker inspect -f '{{.State.Health.Status}}' "$KAFKA_CONTAINER" 2>/dev/null || echo none)"
    # "starting" means it has not finished booting; judging its certificate now
    # would be premature.
    if [[ "$health" != "starting" ]] && ! broker_serves_current_ca; then
        log "the running broker does not serve a certificate matching secrets/ca.pem"
        log "recreating it so it picks up the current keystore"
        recreate=(--force-recreate)
    fi
fi

# ---------------------------------------------------------------------------
# 2. Start. compose blocks on the kafka healthcheck before running kafka-init.
# ---------------------------------------------------------------------------
log "starting ${KAFKA_IMAGE} (KRaft, single node)"
if ! compose up -d "${recreate[@]}"; then
    die "docker compose up failed"
fi

# ---------------------------------------------------------------------------
# 3. Wait for healthy. compose waits too, but a container that dies before the
#    dependency check, or a healthcheck stuck in "starting", must not hang the
#    caller forever.
# ---------------------------------------------------------------------------
log "waiting up to ${READY_TIMEOUT}s for the broker to report healthy"
deadline=$(( $(date +%s) + READY_TIMEOUT ))
while :; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$KAFKA_CONTAINER" 2>/dev/null || echo gone)"
    running="$(docker inspect -f '{{.State.Running}}'      "$KAFKA_CONTAINER" 2>/dev/null || echo false)"

    [[ "$status" == "healthy" ]] && break
    if [[ "$running" != "true" ]]; then
        die "broker container is not running (health=$status)"
    fi
    if (( $(date +%s) >= deadline )); then
        die "broker did not become healthy within ${READY_TIMEOUT}s (health=$status)"
    fi
    sleep 2
done
log "broker is healthy"

# ---------------------------------------------------------------------------
# 4. What version is this, really?
#
#    The image tag in .env is a wish, not a fact: an edited tag, a stale local
#    image or a moved upstream tag can all hand us a broker from a different
#    generation, and every test in the suite would still go green against it.
#    So ask the running container, not the tag.
# ---------------------------------------------------------------------------
log "asking the running broker which version it is"

version_raw="$(docker exec "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | tr -d '\r')" \
    || die "could not run kafka-topics.sh --version inside $KAFKA_CONTAINER"

# `kafka-topics.sh --version` prints e.g. "4.3.1 (Commit:8fca4bd57e76e0ce)".
# Take the first field of the last non-empty line, so a stray banner line in
# front of it does not confuse the parse.
broker_version="$(printf '%s\n' "$version_raw" | awk 'NF { v = $1 } END { print v }')"

if [[ -z "$broker_version" ]]; then
    fail "kafka-topics.sh --version produced nothing usable:"
    printf '%s\n' "$version_raw" | sed 's/^/[up]   /' >&2
    exit 1
fi

broker_major="${broker_version%%.*}"
if [[ ! "$broker_major" =~ ^[0-9]+$ ]]; then
    fail "cannot read a major version out of '${broker_version}'"
    exit 1
fi

if [[ "$broker_major" != "$EXPECT_MAJOR" ]]; then
    fail "this broker is NOT Kafka ${EXPECT_MAJOR}.x"
    {
        printf '[up]   reported version : %s\n' "$broker_version"
        printf '[up]   required major   : %s\n' "$EXPECT_MAJOR"
        printf '[up]   image tag in .env: %s\n' "$KAFKA_IMAGE"
        printf '[up]\n'
        printf '[up]   The suite exists to prove the component works against Kafka\n'
        printf '[up]   %s.x; a pass against anything else means nothing. Fix\n' "$EXPECT_MAJOR"
        printf '[up]   KAFKA_IMAGE_TAG in %s/.env, or set\n' "$SCRIPT_DIR"
        printf '[up]   KAFKA_TEST_EXPECT_BROKER_MAJOR if you really do mean to\n'
        printf '[up]   test another generation.\n'
    } >&2
    exit 1
fi

log "broker reports version ${broker_version} - major ${broker_major}, as required"

# Exported for anything that sources this script, and written down for
# run-tests.sh, which runs it as a child and cannot see the export.
export KAFKA_TEST_BROKER_VERSION="$broker_version"
printf '%s\n' "$broker_version" >"$BROKER_VERSION_FILE" \
    || fail "could not write $BROKER_VERSION_FILE (continuing)"

# ---------------------------------------------------------------------------
# 5. Wait for the init step (topic creation over the SSL listener).
# ---------------------------------------------------------------------------
log "waiting for the init step (topic '${KAFKA_TEST_TOPIC}' over SSL)"
deadline=$(( $(date +%s) + 120 ))
while :; do
    if ! docker container inspect "$INIT_CONTAINER" >/dev/null 2>&1; then
        (( $(date +%s) >= deadline )) && die "init container never appeared"
        sleep 2; continue
    fi
    state="$(docker inspect -f '{{.State.Status}}' "$INIT_CONTAINER")"
    if [[ "$state" == "exited" ]]; then
        code="$(docker inspect -f '{{.State.ExitCode}}' "$INIT_CONTAINER")"
        [[ "$code" == "0" ]] || die "init step failed with exit code $code"
        break
    fi
    if (( $(date +%s) >= deadline )); then
        die "init step did not finish within 120s (state=$state)"
    fi
    sleep 2
done
log "init step completed"

# ---------------------------------------------------------------------------
# 6. Independent TLS proof from the host network, using the very CA bundle the
#    test hands to librdkafka. If this passes, ssl.ca.location is known good.
# ---------------------------------------------------------------------------
log "verifying the TLS handshake with $(openssl_describe)"
WORK_DIR="$SCRIPT_DIR/secrets"
if ! run_openssl_hostnet s_client -connect "localhost:${KAFKA_SSL_PORT}" \
        -CAfile ca.pem -verify_hostname localhost \
        -verify_return_error -brief </dev/null 2>&1 | sed 's/^/[up]   /'; then
    die "TLS handshake against localhost:${KAFKA_SSL_PORT} failed"
fi

# ---------------------------------------------------------------------------
# The version in this banner is the one the broker reported a moment ago, not
# the tag we asked docker for.
cat <<EOF

[up] Kafka ${broker_version} test broker is up.

  image                ${KAFKA_IMAGE}
  reported version     ${broker_version}   (asserted: major == ${EXPECT_MAJOR})
  PLAINTEXT bootstrap  localhost:${KAFKA_PLAINTEXT_PORT}     (debugging only)
  SSL bootstrap        localhost:${KAFKA_SSL_PORT}     <-- use this one
  test topic           ${KAFKA_TEST_TOPIC}

  librdkafka / component configuration:
    security.protocol = ssl
    ssl.ca.location   = ${CA_PEM}

  negative control (must NOT verify the broker):
    ssl.ca.location   = ${OTHER_CA_PEM}

  KAFKA_TEST_BROKER_VERSION=${broker_version}
    also written to ${BROKER_VERSION_FILE}

  Stop with: ${SCRIPT_DIR}/down.sh
EOF
