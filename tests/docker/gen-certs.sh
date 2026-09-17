#!/usr/bin/env bash
#
# Generates the TLS material for the dockerised Kafka 4.x test broker.
#
# Output (all in ./secrets/, gitignored):
#   ca.key                  private key of the throwaway test CA
#   ca.pem                  CA certificate, PEM  <- this is what librdkafka needs
#                                                   (ssl.ca.location / ssl.ca.pem)
#   other-ca.key            private key of a SECOND, completely unrelated CA
#   other-ca.pem            that CA's certificate. It signs nothing - it exists
#                           purely as the negative control for certificate
#                           verification: a test that points ssl.ca.location at
#                           it MUST fail to verify the broker. Without it the
#                           negative control would have to fall back on the
#                           host's system CA bundle, which is neither present
#                           everywhere nor guaranteed to reject the broker.
#   broker.key              broker private key
#   broker.pem              broker certificate, signed by the CA,
#                           CN=localhost, SAN=DNS:localhost,DNS:kafka,IP:127.0.0.1
#   broker.p12              PKCS#12 bundle (key + cert + CA), intermediate
#   kafka.keystore.jks      broker keystore   -> ssl.keystore.location
#   kafka.truststore.jks    broker truststore -> ssl.truststore.location
#   keystore_creds          password files read by the apache/kafka entrypoint
#   key_creds
#   truststore_creds
#   client-ssl.properties   java-client SSL config, used by the compose init step
#
# openssl and keytool are located by _common.sh, which smoke-tests the host
# tools and falls back to running them inside the Kafka image when they are
# absent or broken. No local JDK is required.
#
# Idempotent: a second run validates what is already there and does nothing if
# it is consistent. If anything is missing, expired or signed by a different
# CA, EVERYTHING is regenerated into a temporary directory and swapped in at
# the end - so an interrupted run can never leave a broker certificate and a CA
# bundle that do not match.
#
# Usage:
#   ./gen-certs.sh            regenerate only if needed
#   ./gen-certs.sh --force    always regenerate
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
. "$SCRIPT_DIR/_common.sh"

SECRETS_DIR="$SCRIPT_DIR/secrets"

FORCE=0
case "${1-}" in
    --force|-f) FORCE=1 ;;
    "")         ;;
    *)          echo "usage: $(basename "$0") [--force]" >&2; exit 2 ;;
esac

log()  { printf '[gen-certs] %s\n' "$*"; }
fail() { printf '[gen-certs] ERROR: %s\n' "$*" >&2; exit 1; }

_have docker || fail "docker not found on PATH"

# ---------------------------------------------------------------------------
# Validation of an existing secrets directory.
# Returns 0 when the material is complete, mutually consistent and usable.
# ---------------------------------------------------------------------------
GENERATED_FILES=(
    ca.key ca.pem
    other-ca.key other-ca.pem
    broker.key broker.pem broker.p12
    kafka.keystore.jks kafka.truststore.jks
    keystore_creds key_creds truststore_creds
    client-ssl.properties
)

secrets_are_valid() {
    local f
    for f in "${GENERATED_FILES[@]}"; do
        [[ -s "$SECRETS_DIR/$f" ]] || { log "missing or empty: secrets/$f"; return 1; }
    done

    WORK_DIR="$SECRETS_DIR"

    # The broker certificate must actually chain up to the CA bundle handed to
    # the client. This is the check that catches a mismatched CA.
    if ! run_openssl verify -CAfile ca.pem broker.pem >/dev/null 2>&1; then
        log "broker.pem is not signed by the current ca.pem"; return 1
    fi

    # Still valid for at least another day.
    if ! run_openssl x509 -in broker.pem -noout -checkend 86400 >/dev/null 2>&1; then
        log "broker.pem expires within 24h"; return 1
    fi
    if ! run_openssl x509 -in ca.pem -noout -checkend 86400 >/dev/null 2>&1; then
        log "ca.pem expires within 24h"; return 1
    fi
    if ! run_openssl x509 -in other-ca.pem -noout -checkend 86400 >/dev/null 2>&1; then
        log "other-ca.pem expires within 24h"; return 1
    fi

    # other-ca.pem is only useful as a negative control while it is genuinely
    # unrelated to the broker: if a stale pair ever made it verify, a test that
    # expects verification to FAIL would silently start passing for the wrong
    # reason.
    if run_openssl verify -CAfile other-ca.pem broker.pem >/dev/null 2>&1; then
        log "other-ca.pem verifies broker.pem - it is not an unrelated CA"; return 1
    fi

    # The SANs the host-side test actually connects to.
    local san
    san="$(run_openssl x509 -in broker.pem -noout -ext subjectAltName 2>/dev/null || true)"
    if [[ "$san" != *"DNS:localhost"* || "$san" != *"IP Address:127.0.0.1"* ]]; then
        log "broker.pem SAN does not cover localhost/127.0.0.1"; return 1
    fi

    # The stores must open with the password currently set in .env.
    if ! run_keytool -list -keystore kafka.keystore.jks \
            -storepass "$KAFKA_CERT_PASSWORD" >/dev/null 2>&1; then
        log "kafka.keystore.jks does not open with the configured password"; return 1
    fi
    if ! run_keytool -list -keystore kafka.truststore.jks \
            -storepass "$KAFKA_CERT_PASSWORD" >/dev/null 2>&1; then
        log "kafka.truststore.jks does not open with the configured password"; return 1
    fi

    # Password files must agree with .env too, otherwise the broker would come
    # up with a stale password while the client uses the new one.
    local c
    for c in keystore_creds key_creds truststore_creds; do
        if [[ "$(cat "$SECRETS_DIR/$c")" != "$KAFKA_CERT_PASSWORD" ]]; then
            log "secrets/$c disagrees with KAFKA_CERT_PASSWORD"; return 1
        fi
    done

    return 0
}

# ---------------------------------------------------------------------------
# Generation.
# ---------------------------------------------------------------------------
generate() {
    local tmp
    tmp="$(mktemp -d "$SCRIPT_DIR/.secrets.tmp.XXXXXX")"
    # shellcheck disable=SC2064
    trap "rm -rf '$tmp'" EXIT
    chmod 755 "$tmp"
    WORK_DIR="$tmp"

    local subj_base="/C=RU/O=onec-librdkafka/OU=tests"

    log "generating the test CA"
    run_openssl req -x509 -new -nodes \
        -newkey rsa:2048 -sha256 -days "$KAFKA_CERT_DAYS" \
        -keyout ca.key -out ca.pem \
        -subj "${subj_base}/CN=onec-librdkafka test CA" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -addext "subjectKeyIdentifier=hash" 2>/dev/null

    # A second CA that signs nothing. Different key, different subject, never
    # referenced by the broker: the one thing a certificate-verification
    # negative control can point at and be sure the answer is "no".
    log "generating the unrelated 'other' CA (negative control, signs nothing)"
    run_openssl req -x509 -new -nodes \
        -newkey rsa:2048 -sha256 -days "$KAFKA_CERT_DAYS" \
        -keyout other-ca.key -out other-ca.pem \
        -subj "${subj_base}/CN=onec-librdkafka unrelated CA" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -addext "subjectKeyIdentifier=hash" 2>/dev/null

    log "generating the broker key and CSR (CN=${KAFKA_CERT_CN})"
    run_openssl req -new -nodes \
        -newkey rsa:2048 -sha256 \
        -keyout broker.key -out broker.csr \
        -subj "${subj_base}/CN=${KAFKA_CERT_CN}" \
        -addext "subjectAltName=${KAFKA_CERT_SAN}" 2>/dev/null

    # `x509 -req` ignores CSR extensions unless they are restated, so the SAN is
    # written out explicitly rather than copied across.
    cat >"$tmp/broker-ext.cnf" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=${KAFKA_CERT_SAN}
EOF

    log "signing the broker certificate (SAN=${KAFKA_CERT_SAN})"
    run_openssl x509 -req -sha256 -days "$KAFKA_CERT_DAYS" \
        -in broker.csr \
        -CA ca.pem -CAkey ca.key -CAcreateserial \
        -extfile broker-ext.cnf \
        -out broker.pem 2>/dev/null

    run_openssl verify -CAfile ca.pem broker.pem >/dev/null \
        || fail "freshly signed broker.pem does not verify against ca.pem"

    # The negative control has to be genuinely negative.
    if run_openssl verify -CAfile other-ca.pem broker.pem >/dev/null 2>&1; then
        fail "broker.pem verifies against other-ca.pem - the 'unrelated' CA is not unrelated"
    fi

    log "packing key + certificate chain into PKCS#12"
    run_openssl pkcs12 -export \
        -in broker.pem -inkey broker.key -certfile ca.pem \
        -name kafka-broker \
        -out broker.p12 -passout "pass:${KAFKA_CERT_PASSWORD}"

    log "building kafka.keystore.jks via $(keytool_describe)"
    run_keytool -importkeystore -noprompt \
        -srckeystore broker.p12 -srcstoretype PKCS12 \
        -srcstorepass "$KAFKA_CERT_PASSWORD" -srcalias kafka-broker \
        -destkeystore kafka.keystore.jks -deststoretype JKS \
        -deststorepass "$KAFKA_CERT_PASSWORD" \
        -destkeypass "$KAFKA_CERT_PASSWORD" 2>/dev/null

    log "building kafka.truststore.jks"
    run_keytool -importcert -noprompt \
        -alias onec-librdkafka-test-ca -file ca.pem \
        -keystore kafka.truststore.jks -storetype JKS \
        -storepass "$KAFKA_CERT_PASSWORD" 2>/dev/null

    # The apache/kafka entrypoint reads the store passwords out of files in
    # /etc/kafka/secrets, not out of the environment.
    printf '%s' "$KAFKA_CERT_PASSWORD" >"$tmp/keystore_creds"
    printf '%s' "$KAFKA_CERT_PASSWORD" >"$tmp/key_creds"
    printf '%s' "$KAFKA_CERT_PASSWORD" >"$tmp/truststore_creds"

    # Java-client config for the compose init step and for manual kafka-*.sh use.
    cat >"$tmp/client-ssl.properties" <<EOF
# Generated by gen-certs.sh - for the JVM kafka-*.sh tools, not for librdkafka.
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=${KAFKA_CERT_PASSWORD}
ssl.truststore.type=JKS
ssl.endpoint.identification.algorithm=HTTPS
EOF

    rm -f "$tmp/broker.csr" "$tmp/broker-ext.cnf" "$tmp/ca.srl"
    chmod 644 "$tmp"/*
    chmod 600 "$tmp/ca.key" "$tmp/other-ca.key" "$tmp/broker.key"

    # The old directory only disappears once the new one is complete, so an
    # interrupted run never leaves a mismatched pair behind.
    local old=""
    if [[ -d "$SECRETS_DIR" ]]; then
        old="${SECRETS_DIR}.old.$$"
        mv "$SECRETS_DIR" "$old"
    fi
    mv "$tmp" "$SECRETS_DIR"
    trap - EXIT
    [[ -n "$old" ]] && rm -rf "$old"

    WORK_DIR="$SECRETS_DIR"
    return 0
}

# ---------------------------------------------------------------------------
main() {
    if [[ $FORCE -eq 0 ]] && [[ -d "$SECRETS_DIR" ]] && secrets_are_valid; then
        log "secrets/ is already complete and consistent - nothing to do"
        log "CA bundle for librdkafka:       $SECRETS_DIR/ca.pem"
        log "unrelated CA (negative control): $SECRETS_DIR/other-ca.pem"
        return 0
    fi

    [[ $FORCE -eq 1 ]] && log "--force given, regenerating everything"
    log "using $(openssl_describe)"
    generate

    WORK_DIR="$SECRETS_DIR"
    log "done. Broker certificate:"
    run_openssl x509 -in broker.pem -noout -subject -issuer -dates \
        -ext subjectAltName | sed 's/^/[gen-certs]   /'
    log "CA bundle for librdkafka:       $SECRETS_DIR/ca.pem"
    log "unrelated CA (negative control): $SECRETS_DIR/other-ca.pem"
}

main "$@"
