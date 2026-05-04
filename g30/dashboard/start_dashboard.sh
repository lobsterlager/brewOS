#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${GF_DASHBOARD_PORT:-8002}"
GF_PYTHON="${GF_PYTHON:-python3}"
LOGFILE="${GF_LOG_DIR:-$SCRIPT_DIR/logs}/dashboard_server.log"

export GF_BLE_NAME="${GF_BLE_NAME:-Grain}"
export GF_BLE_ADDRESS="${GF_BLE_ADDRESS:?Set GF_BLE_ADDRESS to your Grainfather BLE address}"

cd "$SCRIPT_DIR"

if command -v lsof >/dev/null 2>&1; then
  if lsof -ti ":$PORT" >/dev/null 2>&1; then
    EXISTING=$(lsof -ti ":$PORT")
    echo "Port $PORT ist bereits belegt (PID $EXISTING). Beende den Prozess..."
    kill "$EXISTING" || true
    sleep 0.5
  fi
fi

echo "Starte Brew-Dashboard auf Port $PORT..."
mkdir -p "$(dirname "$LOGFILE")"
exec "$GF_PYTHON" dashboard_server.py >> "$LOGFILE" 2>&1
