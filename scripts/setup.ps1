$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$baseDir = Split-Path -Parent $PSScriptRoot
$venvDir = Join-Path $baseDir ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$requirements = Join-Path $baseDir "requirements.txt"

if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3 -m venv $venvDir
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    & python -m venv $venvDir
} else {
    throw "Python 3 was not found. Install Python and make py or python available in PATH."
}
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create the virtual environment (exit code $LASTEXITCODE)."
}

& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    throw "Failed to upgrade pip (exit code $LASTEXITCODE)."
}

& $venvPython -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw "Failed to install dependencies (exit code $LASTEXITCODE)."
}

Write-Host "Dependencies installed in $venvDir"
