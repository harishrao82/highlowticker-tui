#!/usr/bin/env bash
# Check/start scallops recorder + viewer. Run from project root.
set -u
cd "$(dirname "$0")"

PY=python3
LOG_DIR=/tmp
TRADES_FILE="$HOME/.scallops_live_trades.jsonl"
STALE_AFTER_SEC=1800   # recorder considered dead if no new trade in 30 min

proc_running() {
    pgrep -f "$1" > /dev/null 2>&1
}

start_proc() {
    local name="$1" script="$2" log="$3"
    if proc_running "$script"; then
        echo "✓ $name already running (pid $(pgrep -f "$script" | head -1))"
    else
        echo "✗ $name not running — starting..."
        nohup $PY "$script" > "$log" 2>&1 &
        disown
        sleep 1
        if proc_running "$script"; then
            echo "  started pid $(pgrep -f "$script" | head -1), log $log"
        else
            echo "  FAILED — check $log"
            tail -5 "$log" 2>/dev/null
        fi
    fi
}

echo "=== Scallops stack check ==="

# 1. Recorder
start_proc "recorder"  "poly_scallops_live_shadow.py" "$LOG_DIR/scallops_recorder.log"

# 2. Freshness check
if [[ -f "$TRADES_FILE" ]]; then
    last_mtime=$(stat -f %m "$TRADES_FILE")
    now_ts=$(date +%s)
    age=$((now_ts - last_mtime))
    if (( age > STALE_AFTER_SEC )); then
        echo "⚠ trades file stale (last update ${age}s ago > ${STALE_AFTER_SEC}s)"
        echo "  killing recorder and restarting..."
        pkill -f poly_scallops_live_shadow.py || true
        sleep 2
        start_proc "recorder" "poly_scallops_live_shadow.py" "$LOG_DIR/scallops_recorder.log"
    else
        echo "✓ trades file fresh (last update ${age}s ago)"
    fi
else
    echo "⚠ trades file does not exist yet — recorder may be warming up"
fi

# 3. Scallops viewer (port 7332)
start_proc "scallops_viewer" "scallops_viewer.py" "$LOG_DIR/scallops_viewer.log"

# 4. Poly viewer (port 7333 — the one at /?hours=6)
start_proc "poly_viewer" "poly_viewer.py" "$LOG_DIR/poly_viewer.log"

echo ""
echo "Scallops trades viewer:  http://localhost:7332"
echo "Poly book viewer:        http://localhost:7333/?hours=6"
echo ""
echo "Tail logs:"
echo "  tail -f $LOG_DIR/scallops_recorder.log"
echo "  tail -f $LOG_DIR/scallops_viewer.log"
echo "  tail -f $LOG_DIR/poly_viewer.log"
