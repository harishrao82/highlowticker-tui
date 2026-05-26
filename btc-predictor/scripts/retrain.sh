#!/usr/bin/env bash
# Full retrain pipeline: rebuild parquet → refit LightGBM → restart live server.
#
# Usage:
#   bash scripts/retrain.sh            # default 90-day half-life
#   bash scripts/retrain.sh 45         # custom half-life in days
#
# Designed to be safely re-runnable. Writes intermediate artifacts in place
# so the running server keeps serving the OLD model until the new one is
# saved + the server is restarted at the end.

set -euo pipefail

HALF_LIFE_DAYS="${1:-90}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PY="/Users/Harish/highlowticker-tui/.venv/bin/python"

echo "════════════════════════════════════════════════════════════"
echo "  BTC predictor — full retrain pipeline"
echo "  $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "════════════════════════════════════════════════════════════"

ZIPS=$(ls ~/binance_ticks/BTCUSDT/*.zip 2>/dev/null | wc -l | tr -d ' ')
FIRST=$(ls ~/binance_ticks/BTCUSDT/*.zip 2>/dev/null | sort | head -1 | sed 's/.*BTCUSDT-trades-//;s/.zip$//')
LAST=$(ls ~/binance_ticks/BTCUSDT/*.zip 2>/dev/null | sort | tail -1 | sed 's/.*BTCUSDT-trades-//;s/.zip$//')
echo ""
echo "  Input:  $ZIPS zips, $FIRST → $LAST"
echo "  Tuning: half_life_days = $HALF_LIFE_DAYS"
echo ""

cd "$ROOT"

echo "─── [1/3] Rebuild training_rows.parquet ────────────────────"
"$VENV_PY" scripts/build_training_dataset.py
echo ""

echo "─── [2/3] Refit LightGBM model ─────────────────────────────"
"$VENV_PY" scripts/train_model.py --half-life-days "$HALF_LIFE_DAYS"
echo ""

echo "─── [3/3] Restart prediction server ────────────────────────"
if pgrep -f "scripts/run_live.py" > /dev/null; then
  echo "  killing existing server…"
  pkill -f "scripts/run_live.py" || true
  sleep 2
fi
echo "  starting fresh server in the background…"
nohup "$VENV_PY" scripts/run_live.py > /tmp/btc-predictor-server.log 2>&1 &
sleep 3
if curl -s -m 3 http://127.0.0.1:8000/health > /dev/null; then
  echo "  ✓ server is up at http://127.0.0.1:8000/"
else
  echo "  ⚠ server didn't answer /health within 3s — check /tmp/btc-predictor-server.log"
fi
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Retrain complete."
echo "  Server log: tail -f /tmp/btc-predictor-server.log"
echo "════════════════════════════════════════════════════════════"
