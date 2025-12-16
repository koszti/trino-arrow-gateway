#!/usr/bin/env bash
set -euo pipefail

SQL=${1:-"SELECT * FROM tpch.sf1.orders LIMIT 1000000"}

TRINO_CLI_BIN="${TRINO_CLI_BIN:-trino}"
TRINO_SERVER="${TRINO_SERVER:-http://localhost:8080}"
TRINO_USER="${TRINO_USER:-trino-arrow-gateway-cli}"

if [[ "$TRINO_CLI_BIN" == */* ]]; then
  if [[ ! -x "$TRINO_CLI_BIN" ]]; then
    echo "TRINO_CLI_BIN not found or not executable: $TRINO_CLI_BIN" >&2
    exit 1
  fi
else
  if ! command -v "$TRINO_CLI_BIN" >/dev/null 2>&1; then
    echo "TRINO_CLI_BIN not found in PATH: $TRINO_CLI_BIN" >&2
    exit 1
  fi
fi

echo "Trino CLI Request"
echo "================="
echo "- Endpoint: $TRINO_SERVER"
echo "- User: $TRINO_USER"
echo "- SQL: $SQL"
echo

echo "Action: execute query in Trino CLI"
echo "=================================="

if command -v /usr/bin/time >/dev/null 2>&1; then
  exec /usr/bin/time -v "$TRINO_CLI_BIN" --server "$TRINO_SERVER" --user "$TRINO_USER" --execute "$SQL" > /dev/null
fi

exec time "$TRINO_CLI_BIN" --server "$TRINO_SERVER" --user "$TRINO_USER" --execute "$SQL" > /dev/null
