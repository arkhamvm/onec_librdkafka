#!/usr/bin/env bash
#
# Stops the Kafka 4.x test broker and removes its data volume.
#
# Usage:
#   ./down.sh                 stop, remove containers and the data volume
#   ./down.sh --purge-certs   the same, plus delete secrets/ (the next up.sh
#                             then mints a brand new CA)
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
. "$SCRIPT_DIR/_common.sh"

COMPOSE_FILE="$SCRIPT_DIR/compose.yml"

PURGE_CERTS=0
case "${1-}" in
    --purge-certs) PURGE_CERTS=1 ;;
    "")            ;;
    *)             echo "usage: $(basename "$0") [--purge-certs]" >&2; exit 2 ;;
esac

log() { printf '[down] %s\n' "$*"; }

log "stopping the test broker and removing its volumes"
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

# up.sh leaves the version the broker actually reported here. Once the broker is
# gone the file is stale, and a stale KAFKA_TEST_BROKER_VERSION is worse than
# none: it would let a later run claim a version nothing verified.
rm -f "$SCRIPT_DIR/secrets/broker-version"

if [[ $PURGE_CERTS -eq 1 ]]; then
    log "removing $SCRIPT_DIR/secrets"
    rm -rf "$SCRIPT_DIR/secrets"
fi

log "done"
