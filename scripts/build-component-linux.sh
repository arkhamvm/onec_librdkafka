#!/usr/bin/env bash
#
# Builds the 1C native component (librdkafka_onec.so) for Linux x86_64.
# Expects lib/linux64/*.a to already exist — run scripts/build-librdkafka-linux.sh first.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build"

rm -rf "$BUILD"
mkdir -p "$BUILD"
cd "$BUILD"

cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j"$(nproc)"

echo
echo ">>> done"
ls -la "$ROOT/out64"
echo
echo ">>> dynamic dependencies (should only be system libraries):"
ldd "$ROOT/out64/librdkafka_onec.so"
