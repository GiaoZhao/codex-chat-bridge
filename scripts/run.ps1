$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$baseDir = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $baseDir ".venv\Scripts\python.exe"
$bridge = Join-Path $baseDir "bridge.py"
$bridgeArgs = @($args)

if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    throw "缺少 .venv，请先运行 .\scripts\setup.ps1。"
}

& $venvPython $bridge @bridgeArgs
exit $LASTEXITCODE
