#!/usr/bin/env bash
# start_trading.sh — restart Coinbase signal detector + Kalshi momentum trader.
# Kills any existing instances first, then starts fresh.
#
# Usage:  ./start_trading.sh
# Stop:   ./start_trading.sh stop
set -u
cd "$(dirname "$0")"

PY=python3
LOG_DIR=/tmp

# ── Helpers ──────────────────────────────────────────────────────────

kill_if_running() {
    local label="$1" pattern="$2"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null)
    if [[ -n "$pids" ]]; then
        echo "  Killing $label (pid $pids)..."
        kill $pids 2>/dev/null
        sleep 1
        # Force-kill if still alive
        pids=$(pgrep -f "$pattern" 2>/dev/null)
        if [[ -n "$pids" ]]; then
            kill -9 $pids 2>/dev/null
            sleep 1
        fi
        echo "  ✓ $label stopped"
    else
        echo "  $label not running"
    fi
}

start_bg() {
    local label="$1" script="$2" log="$3" env_prefix="${4:-}"
    echo "  Starting $label..."
    nohup env $env_prefix caffeinate -di $PY "$script" > "$log" 2>&1 &
    disown
    sleep 2
    if pgrep -f "$script" > /dev/null 2>&1; then
        echo "  ✓ $label running (pid $(pgrep -f "$script" | head -1)), log: $log"
    else
        echo "  ✗ $label FAILED — check $log"
        tail -5 "$log" 2>/dev/null
        return 1
    fi
}

# ── Stop mode ────────────────────────────────────────────────────────

start_if_not_running() {
    # Start a script in the background ONLY if it's not already running.
    # Useful for long-lived recorders we don't want to restart every cycle.
    local label="$1" script="$2" log="$3" env_prefix="${4:-}"
    if pgrep -f "$script" > /dev/null 2>&1; then
        local pid=$(pgrep -f "$script" | head -1)
        echo "  $label already running (pid $pid) — leaving alone"
        return 0
    fi
    echo "  $label not running — starting..."
    nohup env $env_prefix caffeinate -di $PY "$script" > "$log" 2>&1 &
    disown
    sleep 2
    if pgrep -f "$script" > /dev/null 2>&1; then
        echo "  ✓ $label started (pid $(pgrep -f "$script" | head -1)), log: $log"
    else
        echo "  ✗ $label FAILED — check $log"
        tail -5 "$log" 2>/dev/null
    fi
}

if [[ "${1:-}" == "stop" ]]; then
    echo "=== Stopping all ==="
    kill_if_running "Kalshi momentum"        "kalshi_momentum_live.py"
    kill_if_running "Calibration trader"     "calibration_trader.py"
    kill_if_running "Coinbase signals"       "coinbase_signal_detector.py"
    kill_if_running "Scallops recorder"      "poly_scallops_live_shadow.py"
    kill_if_running "Twin fast detector"     "twin_fast_detector.py"
    kill_if_running "Scallops fast detector" "scallops_fast_detector.py"
    # NOTE: btc_recorder is intentionally NOT stopped — it's the long-lived
    # tick archive. Use ./start_trading.sh stop-all if you want to stop it too.
    echo "Done. (btc_recorder.py left running — use 'stop-all' to include it)"
    exit 0
fi

if [[ "${1:-}" == "stop-all" ]]; then
    echo "=== Stopping ALL including btc_recorder ==="
    kill_if_running "Kalshi momentum"        "kalshi_momentum_live.py"
    kill_if_running "Calibration trader"     "calibration_trader.py"
    kill_if_running "Coinbase signals"       "coinbase_signal_detector.py"
    kill_if_running "Scallops recorder"      "poly_scallops_live_shadow.py"
    kill_if_running "Twin fast detector"     "twin_fast_detector.py"
    kill_if_running "Scallops fast detector" "scallops_fast_detector.py"
    kill_if_running "BTC recorder"           "btc_recorder.py"
    echo "Done."
    exit 0
fi

# ── Start mode ───────────────────────────────────────────────────────

echo "=== Stopping existing trading processes (btc_recorder left alone) ==="
kill_if_running "Kalshi momentum"        "kalshi_momentum_live.py"
kill_if_running "Calibration trader"     "calibration_trader.py"
kill_if_running "Coinbase signals"       "coinbase_signal_detector.py"
kill_if_running "Scallops recorder"      "poly_scallops_live_shadow.py"
kill_if_running "Twin fast detector"     "twin_fast_detector.py"
kill_if_running "Scallops fast detector" "scallops_fast_detector.py"
# Also kill any caffeinate wrappers left behind
pkill -f "caffeinate.*kalshi_momentum_live" 2>/dev/null
pkill -f "caffeinate.*calibration_trader" 2>/dev/null
pkill -f "caffeinate.*coinbase_signal_detector" 2>/dev/null
pkill -f "caffeinate.*poly_scallops_live" 2>/dev/null
pkill -f "caffeinate.*twin_fast_detector" 2>/dev/null
pkill -f "caffeinate.*scallops_fast_detector" 2>/dev/null

echo ""
echo "=== Ensuring BTC recorder is running (don't kill, only start if down) ==="
start_if_not_running "BTC recorder" "btc_recorder.py" "$LOG_DIR/btc_recorder.log"

echo ""
echo "=== (Re)building calibration table ==="
$PY build_calibration_table.py 2>&1 | tail -3

echo ""
echo "=== Starting Scallops recorder (REST poll, both Twin + Scallops) ==="
start_bg "Scallops recorder" "poly_scallops_live_shadow.py" "$LOG_DIR/scallops_recorder.log"

echo ""
echo "=== Starting Twin fast detector (PM WS, ~1-2s lag) ==="
start_bg "Twin fast detector" "twin_fast_detector.py" "$LOG_DIR/twin_fast.log"

echo ""
echo "=== Starting Scallops fast detector (PM WS, ~1-2s lag) ==="
start_bg "Scallops fast detector" "scallops_fast_detector.py" "$LOG_DIR/scallops_fast.log"

echo ""
echo "=== Starting Coinbase signal detector ==="
start_bg "Coinbase signals" "coinbase_signal_detector.py" "$LOG_DIR/coinbase_signals.log"

echo ""
echo "=== Starting Kalshi momentum trader ==="
start_bg "Kalshi momentum" "kalshi_momentum_live.py" "$LOG_DIR/kalshi_momentum.log" \
    "STATUS_SLIM=1 STATUS_INTERVAL=15"

echo ""
echo "=== Starting Calibration trader (mispricing exploit) ==="
start_bg "Calibration trader" "calibration_trader.py" "$LOG_DIR/calib_trader.log"

echo ""
echo "════════════════════════════════════════════"
echo "  All processes running."
echo ""
echo "  Scallops recorder   → $LOG_DIR/scallops_recorder.log  (~/.scallops_live_trades.jsonl)"
echo "  Twin fast detector  → $LOG_DIR/twin_fast.log          (~/.twin_fast_signals.jsonl)"
echo "  Scallops fast       → $LOG_DIR/scallops_fast.log      (~/.scallops_fast_signals.jsonl)"
echo "  Coinbase signals    → $LOG_DIR/coinbase_signals.log   (~/.coinbase_signals.jsonl)"
echo "  Kalshi momentum     → $LOG_DIR/kalshi_momentum.log"
echo "  Calibration trader  → $LOG_DIR/calib_trader.log       (~/.calibration_trades.jsonl)"
echo ""
echo "  Stop all:"
echo "    ./start_trading.sh stop"
echo "════════════════════════════════════════════"
