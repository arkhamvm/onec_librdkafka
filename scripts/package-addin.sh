#!/usr/bin/env bash
#
# Packs the built component into the ZIP that 1C loads as an external component
# layout (макет внешней компоненты): a MANIFEST.xml at the root of the archive,
# plus one binary per OS/architecture.
#
# The binaries are NOT built here. Build them first:
#
#   Linux x86_64    scripts/build-linux-portable.sh     -> out64/librdkafka_onec.so
#   Windows x64     scripts/build-component-windows.ps1 -> out64\Release\rdkafka_onec.dll
#   Windows x86     scripts/build-component-windows.ps1 -Arch x86 -> out32\Release\rdkafka_onec.dll
#
# build-linux-portable.sh rather than build-component-linux.sh on purpose: the ordinary
# Linux build carries the glibc of the machine it was built on and will not load on an
# older 1C server, which the platform reports as a bare "ошибка подключения внешней
# компоненты" (BUILD.md section 3.5). Both produce the same path, so either one is picked
# up here - it is the bundle that leaves the building that has to be the portable one.
#
# Windows and Linux binaries come off different machines, so the usual run is to
# copy the .dll over and name it explicitly:
#
#   bash scripts/package-addin.sh --win64 /mnt/share/rdkafka_onec.dll
#
# Whatever is not passed and not found in out64/out32 is left out of the bundle;
# the component simply will not load on that platform. The script prints the
# final contents, so a missing platform is visible at the end of the run.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NAME="rdkafka_onec"
OUT=""
BUILD_TYPE="release"
LINUX64=""
WIN64=""
WIN32=""
# Set by --linux64/--win64/--win32: an explicitly named file that is missing is an
# error, while an autodetected one that is missing is just a platform left out.
declare -A EXPLICIT=()

usage() {
    cat <<'USAGE'
Usage: bash scripts/package-addin.sh [options]

  --name NAME          bundle name in MANIFEST.xml, and the stem of the file names
                       inside the archive. [A-Za-z0-9._-], not starting with a dash
                       (default: rdkafka_onec)
  --out FILE           output archive (default: dist/<name>.zip)
  --build-type TYPE    release | developer (default: release)
  --linux64 PATH       Linux x86_64 .so   (default: out64/librdkafka_onec.so)
  --win64 PATH         Windows x64 .dll   (default: out64/Release/rdkafka_onec.dll,
                       then out64/rdkafka_onec.dll)
  --win32 PATH         Windows x86 .dll   (default: out32/Release/rdkafka_onec.dll,
                       then out32/rdkafka_onec.dll)
  -h, --help           this text
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)       NAME="${2:?--name needs a value}"; shift 2 ;;
        --out)        OUT="${2:?--out needs a value}"; shift 2 ;;
        --build-type) BUILD_TYPE="${2:?--build-type needs a value}"; shift 2 ;;
        --linux64)    LINUX64="${2:?--linux64 needs a value}"; EXPLICIT[linux64]=1; shift 2 ;;
        --win64)      WIN64="${2:?--win64 needs a value}";     EXPLICIT[win64]=1;   shift 2 ;;
        --win32)      WIN32="${2:?--win32 needs a value}";     EXPLICIT[win32]=1;   shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "$BUILD_TYPE" in
    release|developer) ;;
    *) echo "--build-type must be 'release' or 'developer', got '$BUILD_TYPE'" >&2; exit 2 ;;
esac

# NAME ends up in three places at once - an XML attribute value, a file name inside the
# archive, and an argument to zip - so it is restricted to a set that is safe in all
# three instead of being escaped three different ways. A leading dash is what the
# restriction is really for: zip would read "-x_linux_x86_64.so" as its own -x option and
# quietly drop that entry, leaving an archive whose MANIFEST.xml promises a file that is
# not in it.
if [[ ! "$NAME" =~ ^[A-Za-z0-9_][A-Za-z0-9._-]*$ ]]; then
    echo "--name must match [A-Za-z0-9._-] and must not start with a dash or dot, got '$NAME'" >&2
    exit 2
fi

[[ -n "$OUT" ]] || OUT="$ROOT/dist/$NAME.zip"
# zip runs with the staging directory as its cwd, while mkdir/rm/unzip here run with the
# caller's. A relative --out would mean two different files - and the one zip wrote would
# be the one inside the staging directory, which the EXIT trap deletes.
[[ "$OUT" == /* ]] || OUT="$PWD/$OUT"

# First candidate that exists, or the first one as the path to name in the "not found"
# message. The Visual Studio generator is multi-config and CMake appends the config name
# to the output directory, so a DLL built by scripts/build-component-windows.ps1 lands in
# out64\Release (BUILD.md section 4.6), while a Ninja or makefile build puts it directly
# in out64. Both are looked for, because which one it is depends on the generator, not on
# the platform.
first_existing() {
    local candidate
    for candidate in "$@"; do
        if [[ -f "$candidate" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    printf '%s' "$1"
}

[[ -n "$LINUX64" ]] || LINUX64="$(first_existing "$ROOT/out64/librdkafka_onec.so" "$ROOT/out64/Release/librdkafka_onec.so")"
[[ -n "$WIN64" ]]   || WIN64="$(first_existing "$ROOT/out64/Release/rdkafka_onec.dll" "$ROOT/out64/rdkafka_onec.dll")"
[[ -n "$WIN32" ]]   || WIN32="$(first_existing "$ROOT/out32/Release/rdkafka_onec.dll" "$ROOT/out32/rdkafka_onec.dll")"

command -v zip >/dev/null || { echo "zip is required but was not found on PATH." >&2; exit 1; }

# os | arch | source path | name inside the archive
COMP_OS=(); COMP_ARCH=(); COMP_SRC=(); COMP_PATH=()

# A file that is there but is not what the flag says it is would produce a bundle
# that fails only on the customer's machine, at load time, with no explanation, so
# the architecture is read out of the file itself rather than trusted.
expect_file_type() {
    local path="$1" want="$2"
    command -v file >/dev/null || return 0
    local got
    got="$(file -b "$path")"
    # The last branch matters: a case that matches nothing exits 0, so a want value
    # added here later without a pattern would make this check silently pass.
    case "$want" in
        elf64)  [[ "$got" == *"ELF 64-bit"* && "$got" == *"x86-64"* ]] ;;
        pe64)   [[ "$got" == *"PE32+"* && "$got" == *"x86-64"* ]] ;;
        pe32)   [[ "$got" == *"PE32 "* && "$got" == *"80386"* ]] ;;
        *)      echo "expect_file_type: no rule for '$want'" >&2; return 1 ;;
    esac || {
        echo "$path is not a $want binary: $got" >&2
        return 1
    }
}

# GetClassNames() returns this literal, and it is UTF-16 because it crosses the 1C
# ABI - hence -el. It is the cheapest way to tell this component apart from any
# other .so/.dll that happens to sit in out64 (BUILD.md section 7).
expect_component() {
    local path="$1"
    command -v strings >/dev/null || return 0
    # The result is captured rather than piped into a test, and the pipeline's status is
    # discarded: grep -m1 stops at the match and closes the pipe, strings then dies of
    # SIGPIPE with status 141, and under "set -o pipefail" that status would be the
    # pipeline's - failing the check precisely when the string WAS found.
    local found
    found="$(strings -a -el "$path" | grep -m1 '^|KafkaProducer|KafkaConsumer|KafkaAdminClient$' || true)"
    if [[ -z "$found" ]]; then
        echo "$path does not export the Kafka* classes - is it this component?" >&2
        return 1
    fi
}

add() {
    local key="$1" os="$2" arch="$3" src="$4" packed="$5" want="$6"
    if [[ ! -f "$src" ]]; then
        if [[ -n "${EXPLICIT[$key]:-}" ]]; then
            echo "$src does not exist." >&2
            exit 1
        fi
        return 0
    fi
    expect_file_type "$src" "$want"
    expect_component "$src"
    COMP_OS+=("$os"); COMP_ARCH+=("$arch"); COMP_SRC+=("$src"); COMP_PATH+=("$packed")
}

add linux64 Linux   x86_64 "$LINUX64" "${NAME}_linux_x86_64.so"  elf64
add win64   Windows x86_64 "$WIN64"   "${NAME}_win_x86_64.dll"   pe64
add win32   Windows i386   "$WIN32"   "${NAME}_win_i386.dll"     pe32

if [[ ${#COMP_SRC[@]} -eq 0 ]]; then
    echo "Nothing to pack: no built component was found. Build one first - see the header of this script." >&2
    exit 1
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

{
    printf '<?xml version="1.0" encoding="UTF-8"?>\n'
    printf '<bundle xmlns="http://v8.1c.ru/8.2/addin/bundle" name="%s">\n' "$NAME"
    for i in "${!COMP_SRC[@]}"; do
        printf '    <component os="%s" arch="%s" path="%s" type="native" buildType="%s"/>\n' \
            "${COMP_OS[$i]}" "${COMP_ARCH[$i]}" "${COMP_PATH[$i]}" "$BUILD_TYPE"
    done
    printf '</bundle>\n'
} > "$STAGE/MANIFEST.xml"

# include/MANIFEST.xsd cannot validate this file, and it is worth knowing why before
# anyone tries: the schema 1C ships has four independent defects. Every
# <xs:documentation> in it is opened twice and never closed, so it is not even
# well-formed XML; every <xs:simpleType> holds bare <xs:enumeration> children with no
# <xs:restriction> around them; all seven type references in it (type="Component",
# type="OSType", type="ArchType" and so on) are unqualified while the types themselves
# live in the target namespace, so they resolve to nothing; and no top-level
# <xs:element> is declared, so there is no root for a document to be validated against.
# It is kept in include/ exactly as 1C ships it. What is left to check here is that the
# generated file is well-formed - which it is by construction, since every attribute
# value is either fixed above or built from NAME, and NAME is restricted to characters
# that need no XML escaping.
if command -v xmllint >/dev/null; then
    xmllint --noout "$STAGE/MANIFEST.xml" \
        || { echo "generated MANIFEST.xml is not well-formed" >&2; exit 1; }
fi

for i in "${!COMP_SRC[@]}"; do
    cp "${COMP_SRC[$i]}" "$STAGE/${COMP_PATH[$i]}"
done

mkdir -p "$(dirname "$OUT")"
rm -f "$OUT"
# -j so MANIFEST.xml lands at the root of the archive, where the platform reads it;
# -X so the archive carries no uid/gid of the machine that built it; -- so that no file
# name can be read as an option.
(cd "$STAGE" && zip -q -X -j "$OUT" -- MANIFEST.xml "${COMP_PATH[@]}")

echo
echo ">>> $OUT"
# Listing the archive is a report, not part of producing it: unzip is a separate package
# from zip on Debian and Alpine both, and a build container that has one without the
# other must not end up with a valid bundle and a non-zero exit status.
if command -v unzip >/dev/null; then
    unzip -l "$OUT"
else
    echo "(unzip not on PATH - archive contents not listed)"
fi
echo
cat "$STAGE/MANIFEST.xml"
