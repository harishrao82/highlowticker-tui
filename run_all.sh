#!/bin/bash
# Starts paper trader + dashboard, prevents Mac sleep, auto-restarts on crash.
# Run: bash run_all.sh
# Stop: bash run_all.sh stop

cd "$(dirname "$0")"
VENV=".venv/bin/python"
LOG_TRADER="/tmp/btc_strategy.log"
LOG_DASH="/tmp/btc_dashboard.log"
PID_FILE="/tmp/btc_pids"

stop_all() {
    echo "Stopping..."
    if [ -f "$PID_FILE" ]; then
        while read pid; do
            kill "$pid" 2>/dev/null
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    # Kill caffeinate
    pkill -f "caffeinate" 2>/dev/null
    pkill -f "btc_strategy_paper" 2>/dev/null
    pkill -f "btc_dashboard" 2>/dev/null
    echo "All stopped."
    exit 0
}

if [ "$1" = "stop" ]; then
    stop_all
fi

# Prevent stop on existing run
if [ -f "$PID_FILE" ]; then
    echo "Already running (PID file exists). Run: bash run_all.sh stop"
    exit 1
fi

echo "=============================================="
echo "  Paper Trader  (BTC→Kalshi | ETH/SOL/XRP→Poly)"
echo "=============================================="
echo "  Dashboard  : http://localhost:7331"
echo "  Trader log : tail -f $LOG_TRADER"
echo "  Stop       : bash run_all.sh stop"
echo "=============================================="
echo ""

# Prevent Mac from sleeping (display + system idle sleep)
caffeinate -dims &
echo $! >> "$PID_FILE"
echo "Sleep prevention: ON (caffeinate)"

# Auto-restart loop for paper trader
(
    while true; do
        echo "[$(date '+%H:%M:%S')] Starting paper trader..." >> "$LOG_TRADER"
        $VENV trading/analysis/btc_strategy_paper.py >> "$LOG_TRADER" 2>&1
        echo "[$(date '+%H:%M:%S')] Trader crashed — restarting in 5s..." >> "$LOG_TRADER"
        sleep 5
    done
) &
echo $! >> "$PID_FILE"
echo "Paper trader : started (auto-restart enabled)"

sleep 2

# Auto-restart loop for dashboard
(
    while true; do
        echo "[$(date '+%H:%M:%S')] Starting dashboard..." >> "$LOG_DASH"
        $VENV trading/analysis/btc_dashboard.py >> "$LOG_DASH" 2>&1
        echo "[$(date '+%H:%M:%S')] Dashboard crashed — restarting in 5s..." >> "$LOG_DASH"
        sleep 5
    done
) &
echo $! >> "$PID_FILE"
echo "Dashboard    : started → http://localhost:7331"
echo ""
echo "Running. Press Ctrl+C or run 'bash run_all.sh stop' to quit."

# Keep script alive and show trader output
tail -f "$LOG_TRADER"
