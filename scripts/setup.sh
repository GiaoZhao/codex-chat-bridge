#!/bin/zsh
set -eu

SCRIPT_DIR="${0:A:h}"
BASE_DIR="${SCRIPT_DIR:h}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

"${PYTHON_BIN}" -m venv "${BASE_DIR}/.venv"
"${BASE_DIR}/.venv/bin/python" -m pip install --upgrade pip
"${BASE_DIR}/.venv/bin/python" -m pip install -r "${BASE_DIR}/requirements.txt"

echo "Dependencies installed in ${BASE_DIR}/.venv"
