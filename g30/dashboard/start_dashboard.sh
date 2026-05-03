#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="/Users/KriKri/.openclaw/workspace"
PORT=8002
export GF_BLE_NAME="Grain"
export GF_BLE_ADDRESS="4B94A369-C146-CBE4-35BB-258575D08512"
PYTHON="/Users/KriKri/gf-venv/bin/python"
LOGFILE="/Users/KriKri/.openclaw/workspace/logs/dashboard_server.log"
cd "$WORKSPACE"

if command -v lsof >/dev/null 2>&1; then
  if lsof -ti ":$PORT" >/dev/null 2>&1; then
    EXISTING=$(lsof -ti ":$PORT")
    echo "Port $PORT ist bereits belegt (PID $EXISTING). Beende den Prozess..."
    kill "$EXISTING" || true
    sleep 0.5
  fi
fi

echo "Starte Brew-Dashboard auf Port $PORT..."
mkdir -p /Users/KriKri/.openclaw/workspace/logs
exec "$PYTHON" dashboard_server.py >> "$LOGFILE" 2>&1
