#!/usr/bin/env bash
#
# Builds librdkafka and the 1C component inside an old Linux container, so the result
# loads on the distributions 1C actually runs on.
#
# WHY THIS EXISTS
#
# scripts/build-linux-portable.sh's sibling, build-component-linux.sh, builds with
# whatever toolchain the machine happens to have. On a current Ubuntu that produces a
# .so referencing GLIBC_2.38 and GLIBCXX_3.4.32, and glibc's symbol versioning is
# backward compatible only: such a file cannot load on anything older. The 1C platform
# reports that as the unhelpful "ошибка подключения внешней компоненты".
#
# The binding constraint is glibc, and glibc comes with the distribution - it cannot be
# chosen separately. So the oldest runtime that has to be supported becomes the build
# host. libstdc++ is handled separately, by linking it statically.
#
# Ubuntu 20.04 (glibc 2.31, gcc 9.4) is the default base because the upstream README
# names exactly that environment. The produced .so needs glibc >= 2.29 and no
# libstdc++ at all: Ubuntu 20.04+, Debian 11+. For Debian 10 or RHEL 8 (both glibc
# 2.28) pass an older --image.
#
# USAGE
#   scripts/build-linux-portable.sh [options]
#     --image <ref>     base image (default ubuntu:20.04)
#     --version <tag>   librdkafka tag (default v2.15.1)
#     --rebuild-deps    ignore the cached librdkafka archives and build them again
#     --max-glibc <x.y> fail if the result needs more than this (default 2.29)
#     -h, --help
#
# Requires docker. Nothing is installed on the host.
#
set -euo pipefail

IMAGE=ubuntu:20.04
VERSION=v2.15.1
REBUILD_DEPS=0
MAX_GLIBC=2.29

while [ $# -gt 0 ]; do
    case "$1" in
        --image)        IMAGE="$2"; shift 2 ;;
        --version)      VERSION="$2"; shift 2 ;;
        --rebuild-deps) REBUILD_DEPS=1; shift ;;
        --max-glibc)    MAX_GLIBC="$2"; shift 2 ;;
        -h|--help)      sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              echo "error: unknown option $1 (try --help)" >&2; exit 2 ;;
    esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE="$ROOT/.build/portable-$VERSION"

command -v docker >/dev/null || { echo "error: docker is required but not installed" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "error: cannot talk to the docker daemon" >&2; exit 1; }

mkdir -p "$CACHE"
if [ "$REBUILD_DEPS" -eq 1 ]; then
    rm -f "$CACHE/librdkafka-static.a" "$CACHE/librdkafka++.a"
fi

echo ">>> base image   $IMAGE"
echo ">>> librdkafka   $VERSION"
echo ">>> cache        $CACHE"

# The container script. Kept in a quoted heredoc so nothing here is expanded by the
# host shell; the three values it needs arrive as environment variables.
INNER='
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update -qq
# uuid-dev is a component dependency (upstream README lists it); curl and wget are what
# librdkafka'"'"'s mklove uses to fetch the dependency sources, and the base image has
# neither.
apt-get install -y -qq build-essential cmake git perl ca-certificates \
                       curl wget file pkg-config uuid-dev >/dev/null

mkdir -p /build
echo ">>> toolchain    $(gcc --version | head -1)"
echo ">>> glibc        $(ldd --version | head -1 | grep -oE "[0-9]+\.[0-9]+$")"

if [ -f /cache/librdkafka-static.a ] && [ -f /cache/librdkafka++.a ]; then
    echo ">>> librdkafka   reusing cached archives (--rebuild-deps to force)"
else
    echo ">>> librdkafka   cloning $LIBRDKAFKA_VERSION"
    git clone --quiet --depth 1 --branch "$LIBRDKAFKA_VERSION" \
        https://github.com/confluentinc/librdkafka.git /build/rdk
    cd /build/rdk
    # --source-deps-only keeps OpenSSL, zlib, zstd and curl out of the distribution
    # packages and inside librdkafka-static.a, so the component has no external
    # dependency on them either. --disable-gssapi because mklove has no source recipe
    # for Cyrus SASL; SASL PLAIN/SCRAM/OAUTHBEARER still work, they go through OpenSSL.
    echo ">>> librdkafka   configuring (openssl, zlib, zstd, curl from source)"
    ./configure --install-deps --source-deps-only --enable-static \
                --disable-lz4-ext --disable-gssapi >/out/configure.log 2>&1
    echo ">>> librdkafka   building"
    make -j"$(nproc)" >/out/librdkafka-build.log 2>&1
    cp src/librdkafka-static.a src-cpp/librdkafka++.a /cache/
    echo ">>> librdkafka   built and cached"
fi

# /src is read-only and may live on a filesystem without POSIX permissions, so the
# build runs on a copy inside the container.
cp -a /src /build/comp
cd /build/comp
rm -rf build out64
cp /cache/librdkafka-static.a /cache/librdkafka++.a lib/linux64/

mkdir -p build && cd build
# Static libstdc++/libgcc remove the GLIBCXX and GCC_* dependencies entirely, which is
# the other half of making the file portable.
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_SHARED_LINKER_FLAGS="-static-libstdc++ -static-libgcc" \
      .. >/out/cmake.log 2>&1
cmake --build . -j"$(nproc)" >/out/component-build.log 2>&1
echo ">>> component    built"

# /cache and /out are the same host directory, so the archives are already there.
cp /build/comp/out64/librdkafka_onec.so /out/
'

docker run --rm \
    -e LIBRDKAFKA_VERSION="$VERSION" \
    -v "$ROOT:/src:ro" \
    -v "$CACHE:/cache" \
    -v "$CACHE:/out" \
    "$IMAGE" bash -c "$INNER"

SO="$CACHE/librdkafka_onec.so"
[ -f "$SO" ] || { echo "error: the container produced no $SO" >&2; exit 1; }

echo
echo ">>> verifying the result"

# The whole point of this script, asserted rather than assumed.
worst_glibc="$(objdump -T "$SO" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sed 's/GLIBC_//' | sort -uV | tail -1)"
echo "    highest GLIBC required : $worst_glibc  (limit $MAX_GLIBC)"
if [ "$(printf '%s\n%s\n' "$worst_glibc" "$MAX_GLIBC" | sort -V | tail -1)" != "$MAX_GLIBC" ]; then
    echo
    echo "error: the result needs glibc $worst_glibc, above the $MAX_GLIBC limit." >&2
    echo "       These symbols are responsible:" >&2
    objdump -T "$SO" | grep "GLIBC_$worst_glibc" | awk '{print "         " $NF}' | sort -u >&2
    echo "       Build on an older base (--image) or raise --max-glibc deliberately." >&2
    exit 1
fi

cxx_count="$(objdump -T "$SO" | grep -cE 'GLIBCXX|CXXABI' || true)"
echo "    GLIBCXX / CXXABI refs  : $cxx_count  (must be 0)"
if [ "$cxx_count" -ne 0 ]; then
    echo "error: libstdc++ was not linked statically." >&2
    exit 1
fi

echo "    dynamic dependencies   :"
ldd "$SO" | sed 's/^/      /'

# Only now is it safe to publish into the working tree.
mkdir -p "$ROOT/out64" "$ROOT/lib/linux64"
cp "$SO" "$ROOT/out64/librdkafka_onec.so"
cp "$CACHE/librdkafka-static.a" "$CACHE/librdkafka++.a" "$ROOT/lib/linux64/"

echo
echo ">>> done"
echo "    $ROOT/out64/librdkafka_onec.so"
echo "    $ROOT/lib/linux64/librdkafka-static.a"
echo "    $ROOT/lib/linux64/librdkafka++.a"
echo
echo "    Verify it end to end with:  tests/run-tests.sh"
