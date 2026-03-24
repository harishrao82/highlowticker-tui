#!/bin/bash
# start.sh — launch HighLow TUI in a new Terminal window if not already running.
# Intended to be called from cron at 9:30 AM ET Mon–Fri.

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

# Already running? Do nothing.
if pgrep -f "python.*app\.py" > /dev/null 2>&1; then
    echo "HighLow TUI already running — skipping."
    exit 0
fi

VENV_ACTIVATE="$(dirname "$APP_DIR")/.venv/bin/activate"

osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '$APP_DIR' && source '$VENV_ACTIVATE' && python app.py"
end tell
EOF
