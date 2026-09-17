# shellcheck shell=bash
#
# Shared helpers for gen-certs.sh / up.sh / down.sh. Not executable on its own.
#
# The interesting part is tool selection. Two things cannot be taken for
# granted on a developer box:
#
#   * keytool - there may be no JDK at all;
#   * openssl - there may be a *broken* one. On the reference machine
#     /usr/bin/openssl is the Ubuntu 3.0.13 binary but ld.so resolves
#     libcrypto.so.3 to the hand-built /usr/local/ssl/lib64/libcrypto.so.3
#     (3.3.1, installed by scripts/build-librdkafka-linux.sh). That ABI
#     mismatch segfaults on the X509v3 extension path, i.e. exactly on the
#     `-addext` calls this test rig needs. `openssl version` still works, so
#     merely probing for the binary is not enough - it has to be smoke-tested.
#
# Both tools therefore fall back to running inside the Kafka image, which
# carries a JDK 21 keytool and OpenSSL 3.5.7.
#
# Env overrides:
#   ONEC_OPENSSL=/path/to/openssl   try this binary first
#   ONEC_FORCE_DOCKER_TOOLS=1       skip the host entirely, always use the image

set -euo pipefail

DOCKER_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
set -a; . "$DOCKER_DIR/.env"; set +a

KAFKA_IMAGE="apache/kafka:${KAFKA_IMAGE_TAG}"

# Directory the tool helpers operate in. Callers set it; every helper is
# invoked with bare relative file names so the local and docker paths behave
# identically.
WORK_DIR="${WORK_DIR:-$DOCKER_DIR}"

OPENSSL_MODE=""
OPENSSL_BIN=""
KEYTOOL_MODE=""

_have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# openssl
# ---------------------------------------------------------------------------

# Decides whether an openssl binary can be trusted.
#
# Two gates, because one is not enough:
#
#   1. The version string reports both the version the binary was built
#      against and the version actually loaded at runtime:
#        OpenSSL 3.0.13 30 Jan 2024 (Library: OpenSSL 3.3.1 4 Jun 2024)
#      When those differ the binary is running against a foreign libcrypto and
#      anything may happen. Reject it outright.
#
#   2. A live probe of the -addext path, checked on EXIT STATUS, not just on
#      the output. The mismatched /usr/bin/openssl on the reference box writes
#      a perfectly valid certificate and only then dies with SIGSEGV in its
#      exit cleanup - so a probe that merely inspects the produced file passes
#      while every real call still kills a `set -e` script.
_openssl_works() {
    local bin="$1" probe rc=0 ver built loaded

    ver="$("$bin" version 2>/dev/null)" || return 1
    if [[ "$ver" == *"(Library:"* ]]; then
        built="$(printf '%s' "$ver"  | sed -n 's/^OpenSSL \([^ ]*\).*/\1/p')"
        loaded="$(printf '%s' "$ver" | sed -n 's/.*(Library: OpenSSL \([^ ]*\).*/\1/p')"
        [[ -n "$built" && -n "$loaded" && "$built" == "$loaded" ]] || return 1
    fi

    probe="$(mktemp -d)"
    if ! "$bin" req -x509 -new -nodes -newkey rsa:2048 -sha256 -days 1 \
            -keyout "$probe/p.key" -out "$probe/p.pem" -subj "/CN=probe" \
            -addext "basicConstraints=critical,CA:TRUE" \
            -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
            >/dev/null 2>&1; then
        rc=1
    fi
    if [[ $rc -eq 0 ]] && ! "$bin" x509 -in "$probe/p.pem" -noout \
            -ext subjectAltName 2>/dev/null | grep -q "127.0.0.1"; then
        rc=1
    fi
    rm -rf "$probe"
    return $rc
}

pick_openssl() {
    [[ -n "$OPENSSL_MODE" ]] && return 0
    if [[ "${ONEC_FORCE_DOCKER_TOOLS:-0}" == "1" ]]; then
        OPENSSL_MODE="docker"; OPENSSL_BIN="$KAFKA_IMAGE"; return 0
    fi
    local cand
    for cand in "${ONEC_OPENSSL:-}" openssl /usr/local/ssl/bin/openssl /usr/bin/openssl; do
        [[ -n "$cand" ]] || continue
        _have "$cand" || continue
        if _openssl_works "$cand"; then
            OPENSSL_MODE="local"
            OPENSSL_BIN="$(command -v "$cand")"
            return 0
        fi
    done
    _have docker || { echo "no usable openssl and no docker" >&2; return 1; }
    OPENSSL_MODE="docker"
    OPENSSL_BIN="$KAFKA_IMAGE"
    return 0
}

# Runs openssl with cwd == $WORK_DIR. Pass bare relative file names.
run_openssl() {
    pick_openssl
    if [[ "$OPENSSL_MODE" == "local" ]]; then
        ( cd "$WORK_DIR" && "$OPENSSL_BIN" "$@" )
    else
        docker run --rm -i \
            --user "$(id -u):$(id -g)" \
            --volume "$WORK_DIR:/work" \
            --workdir /work \
            --entrypoint openssl \
            "$KAFKA_IMAGE" "$@"
    fi
}

# Same, but joined to the host network so that "localhost" means the host even
# when openssl has to run in a container. Used for the TLS handshake check.
run_openssl_hostnet() {
    pick_openssl
    if [[ "$OPENSSL_MODE" == "local" ]]; then
        ( cd "$WORK_DIR" && "$OPENSSL_BIN" "$@" )
    else
        docker run --rm -i \
            --network host \
            --user "$(id -u):$(id -g)" \
            --volume "$WORK_DIR:/work" \
            --workdir /work \
            --entrypoint openssl \
            "$KAFKA_IMAGE" "$@"
    fi
}

openssl_describe() {
    pick_openssl
    if [[ "$OPENSSL_MODE" == "local" ]]; then
        printf '%s (%s)' "$OPENSSL_BIN" "$("$OPENSSL_BIN" version)"
    else
        printf 'openssl inside %s' "$KAFKA_IMAGE"
    fi
}

# ---------------------------------------------------------------------------
# keytool
# ---------------------------------------------------------------------------

pick_keytool() {
    [[ -n "$KEYTOOL_MODE" ]] && return 0
    if [[ "${ONEC_FORCE_DOCKER_TOOLS:-0}" == "1" ]]; then
        KEYTOOL_MODE="docker"; return 0
    fi
    if _have keytool && keytool -help >/dev/null 2>&1; then
        KEYTOOL_MODE="local"
    else
        _have docker || { echo "no keytool and no docker" >&2; return 1; }
        KEYTOOL_MODE="docker"
    fi
    return 0
}

# Runs keytool with cwd == $WORK_DIR. Pass bare relative file names.
run_keytool() {
    pick_keytool
    if [[ "$KEYTOOL_MODE" == "local" ]]; then
        ( cd "$WORK_DIR" && keytool "$@" )
    else
        docker run --rm -i \
            --user "$(id -u):$(id -g)" \
            --volume "$WORK_DIR:/work" \
            --workdir /work \
            --entrypoint keytool \
            "$KAFKA_IMAGE" "$@"
    fi
}

keytool_describe() {
    pick_keytool
    if [[ "$KEYTOOL_MODE" == "local" ]]; then
        printf '%s' "$(command -v keytool)"
    else
        printf 'keytool inside %s' "$KAFKA_IMAGE"
    fi
}
