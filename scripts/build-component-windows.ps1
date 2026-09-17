<#
.SYNOPSIS
    Builds the 1C native component (rdkafka_onec.dll) for Windows with MSVC.

.DESCRIPTION
    Expects lib\win64\*.lib (or lib\win32\*.lib) to already exist - run
    scripts\build-librdkafka-windows.ps1 first.

    The component links the static /MT CRT so that the resulting DLL can be dropped
    into a 1C installation without shipping the VC++ redistributable.

.PARAMETER Arch
    x64 (default) or x86.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\build-component-windows.ps1
#>
[CmdletBinding()]
param(
    [ValidateSet('x64', 'x86')][string]$Arch = 'x64'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Root      = Split-Path -Parent $PSScriptRoot
$CmakeArch = if ($Arch -eq 'x64') { 'x64' } else { 'Win32' }
$OutSubDir = if ($Arch -eq 'x64') { 'out64' } else { 'out32' }
$BuildDir  = Join-Path $Root "build-$Arch"

if (-not (Get-Command cmake -ErrorAction SilentlyContinue)) {
    throw 'cmake is required but was not found on PATH.'
}

if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

# CMP0091 NEW + MultiThreaded => /MT, matching the vcpkg *-windows-static dependencies.
cmake -S $Root -B $BuildDir -A $CmakeArch `
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW `
    -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded
if ($LASTEXITCODE -ne 0) { throw 'cmake configure failed' }

cmake --build $BuildDir --config Release --parallel
if ($LASTEXITCODE -ne 0) { throw 'cmake build failed' }

Write-Host ''
Write-Host '>>> done' -ForegroundColor Green
Get-ChildItem (Join-Path $Root $OutSubDir) -ErrorAction SilentlyContinue
