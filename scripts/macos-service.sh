#!/bin/zsh
set -eu

SCRIPT_DIR="${0:A:h}"
BASE_DIR="${SCRIPT_DIR:h}"
PYTHON_BIN="${BASE_DIR}/.venv/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing .venv. Run scripts/setup.sh first." >&2
  exit 2
fi

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/macos_service.py" "$@"
