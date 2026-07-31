#!/bin/zsh
set -eu

SCRIPT_DIR="${0:A:h}"
BASE_DIR="${SCRIPT_DIR:h}"

if [[ ! -x "${BASE_DIR}/.venv/bin/python" ]]; then
  echo "Missing .venv. Run scripts/setup.sh first." >&2
  exit 2
fi

if [[ "${CODEX_QQ_KEEP_AWAKE:-1}" == "1" ]] && command -v caffeinate >/dev/null 2>&1; then
  exec caffeinate -i "${BASE_DIR}/.venv/bin/python" "${BASE_DIR}/bridge.py" "$@"
fi

exec "${BASE_DIR}/.venv/bin/python" "${BASE_DIR}/bridge.py" "$@"
