#!/bin/zsh
set -eu

SCRIPT_DIR="${0:A:h}"
BASE_DIR="${SCRIPT_DIR:h}"

if [[ ! -x "${BASE_DIR}/.venv/bin/python" ]]; then
  echo "缺少 .venv，请先运行 scripts/setup.sh。" >&2
  exit 2
fi

if [[ "${CODEX_QQ_KEEP_AWAKE:-1}" == "1" ]] && command -v caffeinate >/dev/null 2>&1; then
  exec caffeinate -i "${BASE_DIR}/.venv/bin/python" "${BASE_DIR}/bridge.py" "$@"
fi

exec "${BASE_DIR}/.venv/bin/python" "${BASE_DIR}/bridge.py" "$@"
