#!/usr/bin/env bash
#
# Builds a self-contained static librdkafka for Linux x86_64 and installs the
# resulting archives into lib/linux64/ plus the public headers into src/.
#
# OpenSSL, zlib, zstd and curl are built from source by mklove and archived into
# librdkafka-static.a, so the produced 1C component has no external .so dependencies
# beyond glibc/libstdc++/pthread/dl/m/rt.
#
# Usage: scripts/build-librdkafka-linux.sh [version-tag]
#        scripts/build-librdkafka-linux.sh v2.15.1
#
set -euo pipefail

VERSION="${1:-v2.15.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/lib/linux64"

# The dependency builds run `install(1)` with explicit modes, which fails on filesystems
# without POSIX permissions (NTFS/exFAT via fuseblk, most Windows-shared mounts). When the
# checkout lives on such a mount, build in $TMPDIR instead and copy only the artifacts back.
FSTYPE="$(stat -f -c '%T' "$ROOT")"
case "$FSTYPE" in
    fuseblk|ntfs|exfat|msdos|vfat|cifs|smb2|9p)
        WORK="${TMPDIR:-/tmp}/onec-librdkafka-build/librdkafka-$VERSION"
        echo ">>> $ROOT is on $FSTYPE (no POSIX permissions); building in $WORK instead"
        ;;
    *)
        WORK="$ROOT/.build/librdkafka-$VERSION"
        ;;
esac

if [ "$(uname -m)" != "x86_64" ]; then
    echo "error: this script targets x86_64; got $(uname -m)" >&2
    exit 1
fi

for tool in git gcc g++ make perl; do
    command -v "$tool" >/dev/null || { echo "error: $tool is required but not installed" >&2; exit 1; }
done

if [ ! -d "$WORK" ]; then
    echo ">>> cloning librdkafka $VERSION"
    mkdir -p "$(dirname "$WORK")"
    git clone --depth 1 --branch "$VERSION" https://github.com/confluentinc/librdkafka.git "$WORK"
fi

cd "$WORK"

echo ">>> configuring (deps from source: openssl, zlib, zstd, curl)"
# --source-deps-only forbids picking up system libraries, which is what makes the
# archive self-contained and reproducible across distros.
# --disable-lz4-ext uses librdkafka's bundled lz4 instead of a system one.
./configure --install-deps --source-deps-only --enable-static --disable-lz4-ext

echo ">>> building"
make -j"$(nproc)"

echo ">>> installing into $DEST and $ROOT/src"
mkdir -p "$DEST"
cp -v src/librdkafka-static.a "$DEST/librdkafka-static.a"
cp -v src-cpp/librdkafka++.a  "$DEST/librdkafka++.a"
# Headers are vendored pristine; the static-link switch lives in CMakeLists.txt.
cp -v src/rdkafka.h       "$ROOT/src/rdkafka.h"
cp -v src-cpp/rdkafkacpp.h "$ROOT/src/rdkafkacpp.h"

echo
echo ">>> done"
grep -m1 'define RD_KAFKA_VERSION ' "$ROOT/src/rdkafka.h"
ls -la "$DEST"
