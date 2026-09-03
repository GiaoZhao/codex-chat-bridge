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
    throw "未找到 Python 3。请安装 Python，并确保可通过 PATH 调用 py 或 python。"
}
if ($LASTEXITCODE -ne 0) {
    throw "创建虚拟环境失败（退出码 $LASTEXITCODE）。"
}

& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    throw "升级 pip 失败（退出码 $LASTEXITCODE）。"
}

& $venvPython -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw "安装依赖失败（退出码 $LASTEXITCODE）。"
}

Write-Host "依赖已安装到 $venvDir"
