<#
.SYNOPSIS
    Builds a static librdkafka for Windows with MSVC and installs the resulting
    .lib files into lib\win64 (or lib\win32) plus the public headers into src\.

.DESCRIPTION
    Dependencies (OpenSSL, zlib, zstd, curl) are provided by vcpkg using a *-windows-static
    triplet, so everything ends up linked with the static /MT CRT - which is what the
    component itself is built with. vcpkg is bootstrapped into .build\vcpkg if not supplied.

    Run from a "x64 Native Tools Command Prompt for VS" / Developer PowerShell,
    or make sure cmake.exe and MSVC are otherwise on PATH.

.PARAMETER Version
    librdkafka git tag to build. Default: v2.15.1

.PARAMETER Arch
    x64 (default) or x86.

.PARAMETER VcpkgRoot
    Existing vcpkg installation to reuse. Default: .build\vcpkg (bootstrapped on demand).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1
    powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1 -Arch x86
#>
[CmdletBinding()]
param(
    [string]$Version = 'v2.15.1',
    [ValidateSet('x64', 'x86')][string]$Arch = 'x64',
    [string]$VcpkgRoot = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Root    = Split-Path -Parent $PSScriptRoot
$BuildDir = Join-Path $Root '.build'
$Triplet = "$Arch-windows-static"
$CmakeArch = if ($Arch -eq 'x64') { 'x64' } else { 'Win32' }
$DestSubDir = if ($Arch -eq 'x64') { 'lib\win64' } else { 'lib\win32' }
$DestDir = Join-Path $Root $DestSubDir

foreach ($tool in @('git', 'cmake')) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool is required but was not found on PATH."
    }
}

New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

# --- vcpkg -----------------------------------------------------------------
if ([string]::IsNullOrWhiteSpace($VcpkgRoot)) {
    $VcpkgRoot = Join-Path $BuildDir 'vcpkg'
}
if (-not (Test-Path (Join-Path $VcpkgRoot '.git'))) {
    Write-Host '>>> cloning vcpkg' -ForegroundColor Cyan
    git clone https://github.com/microsoft/vcpkg.git $VcpkgRoot
    if ($LASTEXITCODE -ne 0) { throw 'git clone of vcpkg failed' }
}
$VcpkgExe = Join-Path $VcpkgRoot 'vcpkg.exe'
if (-not (Test-Path $VcpkgExe)) {
    Write-Host '>>> bootstrapping vcpkg' -ForegroundColor Cyan
    & (Join-Path $VcpkgRoot 'bootstrap-vcpkg.bat') -disableMetrics
    if ($LASTEXITCODE -ne 0) { throw 'vcpkg bootstrap failed' }
}

Write-Host ">>> installing dependencies for $Triplet (this takes a while on a cold cache)" -ForegroundColor Cyan
# curl[core,ssl] gives us the OIDC/OAUTHBEARER transport; openssl covers SSL + SASL SCRAM.
& $VcpkgExe install "zlib:$Triplet" "zstd:$Triplet" "openssl:$Triplet" "curl[core,ssl]:$Triplet" --recurse
if ($LASTEXITCODE -ne 0) { throw 'vcpkg install failed' }

# --- librdkafka ------------------------------------------------------------
$Src = Join-Path $BuildDir "librdkafka-$Version"
if (-not (Test-Path $Src)) {
    Write-Host ">>> cloning librdkafka $Version" -ForegroundColor Cyan
    git clone --depth 1 --branch $Version https://github.com/confluentinc/librdkafka.git $Src
    if ($LASTEXITCODE -ne 0) { throw "git clone of librdkafka $Version failed" }
}

$Out = Join-Path $Src "build-$Arch"
# The build tree is recreated on every run, as build-component-windows.ps1 does with its own.
# vcpkg refuses to reconfigure a tree whose manifest mode changed ("delete the build directory
# and reconfigure"), and a cache left by an earlier run must not decide what ends up archived
# into lib\win64. Stage 1 only runs when librdkafka changes, so the full rebuild is cheap.
if (Test-Path $Out) { Remove-Item -Recurse -Force $Out }
Write-Host '>>> configuring librdkafka' -ForegroundColor Cyan
# CMP0091 must be NEW for CMAKE_MSVC_RUNTIME_LIBRARY to take effect: librdkafka still
# declares cmake_minimum_required(VERSION 3.5), which would otherwise pin the old behaviour.
# ENABLE_LZ4_EXT=OFF uses librdkafka's bundled lz4, matching the Linux build.
# VCPKG_MANIFEST_MODE=OFF: librdkafka ships a vcpkg.json, and with it the toolchain would
# install a second, baseline-pinned copy of the dependencies into build-$Arch\vcpkg_installed
# and compile against that - while the libraries copied into lib\win64 below come from the
# classic-mode tree installed above. Headers and archives must come from the same build.
# VCPKG_INSTALLED_DIR is spelled out for the same reason: it pins the tree librdkafka compiles
# against to the one the archives are copied from below, instead of leaving it to vcpkg.cmake,
# which takes any value it finds in CMakeCache.txt over the classic default.
# The -D arguments that carry a variable are quoted on purpose: PowerShell reads an unquoted
# -DNAME=$var as a parameter token and passes "$var" to cmake literally.
# WITHOUT_WIN32_CONFIG=OFF: on _WIN32, rd.h takes its feature switches from src\win32_config.h
# and never from the generated config.h. librdkafka's CMake switches that header's block off by
# default and defines a partial replacement in which WITH_SNAPPY comes out as 0 and WITH_CURL,
# WITH_OAUTHBEARER_OIDC and WITH_HDRHISTOGRAM are not defined at all. The library still builds,
# but it cannot decompress snappy batches ("snappy not enabled at build time") and rejects the
# OIDC settings. The header's own block - what upstream's MSVC project builds with - gives the
# same feature set as the Linux build. It includes WITH_HDRHISTOGRAM, whose source file CMake
# only compiles when that variable is set, and CMake's libm probe for it always fails on Windows.
cmake -S $Src -B $Out -A $CmakeArch `
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW `
    -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded `
    "-DCMAKE_TOOLCHAIN_FILE=$VcpkgRoot\scripts\buildsystems\vcpkg.cmake" `
    "-DVCPKG_TARGET_TRIPLET=$Triplet" `
    -DVCPKG_MANIFEST_MODE=OFF `
    "-DVCPKG_INSTALLED_DIR=$VcpkgRoot\installed" `
    -DRDKAFKA_BUILD_STATIC=ON `
    -DRDKAFKA_BUILD_EXAMPLES=OFF `
    -DRDKAFKA_BUILD_TESTS=OFF `
    -DWITH_SSL=ON `
    -DWITH_CURL=ON `
    -DWITH_ZLIB=ON `
    -DWITH_ZSTD=ON `
    -DWITH_SASL=ON `
    -DWITHOUT_WIN32_CONFIG=OFF `
    -DWITH_HDRHISTOGRAM=ON `
    -DENABLE_LZ4_EXT=OFF
if ($LASTEXITCODE -ne 0) { throw 'cmake configure failed' }

Write-Host '>>> building librdkafka (Release)' -ForegroundColor Cyan
cmake --build $Out --config Release --parallel
if ($LASTEXITCODE -ne 0) { throw 'cmake build failed' }

# --- install ---------------------------------------------------------------
Write-Host ">>> installing into $DestDir" -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
# Wipe stale archives: a mismatch between the headers in src\ and the .lib files here
# produces link errors at best and silent ABI corruption at worst.
Get-ChildItem -Path $DestDir -Filter *.lib -ErrorAction SilentlyContinue | Remove-Item -Force

$produced = @()
foreach ($name in @('rdkafka.lib', 'rdkafka++.lib')) {
    $found = Get-ChildItem -Path $Out -Filter $name -Recurse -File |
             Where-Object { $_.FullName -match '\\Release\\' } |
             Select-Object -First 1
    if (-not $found) { throw "$name was not produced by the librdkafka build" }
    Copy-Item $found.FullName (Join-Path $DestDir $name) -Force
    $produced += $name
}

# vcpkg static dependencies that librdkafka references but does not archive in.
# vcpkg renames archives between port versions - zlib 1.3.2 installs zs.lib where earlier
# ports installed zlib.lib - so each one is looked up under the names vcpkg has used and
# copied under the single name CMakeLists.txt expects. All five are required: the configure
# step above turns SSL, CURL, ZLIB and ZSTD on, and a missing archive would otherwise only
# show up an hour later as unresolved symbols when the component links.
$VcpkgLib = Join-Path $VcpkgRoot "installed\$Triplet\lib"
$deps = [ordered]@{
    'libssl.lib'    = @('libssl.lib')
    'libcrypto.lib' = @('libcrypto.lib')
    'libcurl.lib'   = @('libcurl.lib')
    'zlib.lib'      = @('zs.lib', 'zlib.lib')
    'zstd.lib'      = @('zstd.lib')
}
foreach ($dep in $deps.Keys) {
    $path = $deps[$dep] | ForEach-Object { Join-Path $VcpkgLib $_ } |
            Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $path) {
        throw "vcpkg dependency $dep not found in $VcpkgLib (looked for: $($deps[$dep] -join ', '))"
    }
    Copy-Item $path (Join-Path $DestDir $dep) -Force
    $produced += $dep
}

Copy-Item (Join-Path $Src 'src\rdkafka.h')        (Join-Path $Root 'src\rdkafka.h')    -Force
Copy-Item (Join-Path $Src 'src-cpp\rdkafkacpp.h') (Join-Path $Root 'src\rdkafkacpp.h') -Force

Write-Host ''
Write-Host '>>> done' -ForegroundColor Green
Select-String -Path (Join-Path $Root 'src\rdkafka.h') -Pattern 'define RD_KAFKA_VERSION ' | Select-Object -First 1
Get-ChildItem $DestDir
