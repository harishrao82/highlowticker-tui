#!/bin/bash
# start.sh — launch HighLow TUI in a new Terminal window if not already running.
# Intended to be called from cron at 9:30 AM ET Mon–Fri.
#
# Usage:
#   start.sh           — start if not running (no-op otherwise)
#   start.sh restart   — kill the running instance, then start a fresh one
#   start.sh stop      — kill the running instance and exit

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_ACTIVATE="$(dirname "$APP_DIR")/.venv/bin/activate"

is_running() {
    pgrep -f "python.*app\.py" > /dev/null 2>&1
}

stop_running() {
    if is_running; then
        echo "Stopping HighLow TUI..."
        pkill -f "python.*app\.py"
        # Wait up to 5s for graceful exit, then force kill
        for _ in 1 2 3 4 5; do
            is_running || return 0
            sleep 1
        done
        echo "Process did not exit after 5s — forcing."
        pkill -9 -f "python.*app\.py"
        sleep 1
    fi
}

start_app() {
    osascript <<EOF
tell application "Terminal"
    activate
    do script "cd '$APP_DIR' && source '$VENV_ACTIVATE' && python app.py"
end tell
EOF
}

case "${1:-start}" in
    restart)
        stop_running
        start_app
        ;;
    stop)
        stop_running
        ;;
    start|"")
        if is_running; then
            echo "HighLow TUI already running — skipping."
            exit 0
        fi
        start_app
        ;;
    *)
        echo "Usage: $0 [start|restart|stop]" >&2
        exit 1
        ;;
esac
