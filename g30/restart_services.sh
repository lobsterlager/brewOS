#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="/Users/KriKri/.openclaw/workspace"
PORT=8002
LOG_DIR="$WORKSPACE/logs"
LOGFILE="$LOG_DIR/dashboard_server.log"

echo "Stoppe laufenden Brew-Flow..."
if command -v pkill >/dev/null 2>&1; then
  pkill -f "gf_brew_flow.py" || true
fi

echo "Stoppe Dashboard-Server (Port $PORT)..."
if command -v lsof >/dev/null 2>&1; then
  if lsof -ti ":$PORT" >/dev/null 2>&1; then
    lsof -ti ":$PORT" | xargs -I{} kill {} || true
    sleep 0.5
  fi
fi

echo "Starte Dashboard-Server neu..."
mkdir -p "$LOG_DIR"
nohup "$WORKSPACE/dashboard/start_dashboard.sh" >> "$LOGFILE" 2>&1 &
echo "Dashboard läuft. Öffne: http://localhost:$PORT/dashboard/index.html"
echo "Brew-Flow bitte über das Dashboard mit 'Brau starten' starten."
