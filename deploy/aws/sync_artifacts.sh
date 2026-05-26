#!/bin/bash
# sync_artifacts.sh — push model + persistent state from Mac to EC2.
#
# Usage:
#   bash deploy/aws/sync_artifacts.sh <PUBLIC-IP>
#   bash deploy/aws/sync_artifacts.sh <PUBLIC-IP> ~/.ssh/<key>.pem   # custom key
#
# What it ships (rsync; idempotent):
#   - btc-predictor/src/model/*.json + model.pkl   (the trained LightGBM)
#   - btc-predictor/data/processed/baselines.json  (training-set stats)
#   - btc-predictor/data/predictions.jsonl         (chart + rehydrate state)
#   - btc-predictor/data/window_pnl.jsonl          (auto-tuner input)
#   - btc-predictor/data/arb_trades.jsonl          (per-trade ledger)
#   - btc-predictor/data/execution_results.jsonl   (live-order ledger)
#   - .env at repo root (Kalshi creds — file is chmod 600 on both sides)
#
# Does NOT ship:
#   - btc-predictor/data/processed/<training parquets>  (~133MB, only used
#     for offline retraining, irrelevant to a live server)
#   - btc-predictor/notebooks/                          (dev tooling)
#   - btc-predictor/.venv/                              (built on EC2)

set -euo pipefail

IP="${1:-}"
KEY="${2:-$HOME/.ssh/kalshi-bot.pem}"
USER="${EC2_USER:-ec2-user}"

if [ -z "$IP" ]; then
    echo "Usage: $0 <PUBLIC-IP> [path/to/key.pem]"
    exit 1
fi
if [ ! -f "$KEY" ]; then
    echo "✗ SSH key not found: $KEY"
    echo "  Set EC2_USER=ubuntu and/or pass the .pem path as 2nd arg."
    exit 1
fi

LOCAL_REPO="$(cd "$(dirname "$0")/../.." && pwd)"
LOCAL_PRED="$LOCAL_REPO/btc-predictor"
REMOTE_REPO="/home/$USER/highlowticker-tui"
REMOTE_PRED="$REMOTE_REPO/btc-predictor"

echo "═══════════════════════════════════════════════════════════"
echo " sync_artifacts.sh"
echo "  local repo:  $LOCAL_REPO"
echo "  remote:      $USER@$IP:$REMOTE_REPO"
echo "  ssh key:     $KEY"
echo "═══════════════════════════════════════════════════════════"

RSYNC="rsync -e 'ssh -i $KEY -o StrictHostKeyChecking=accept-new' -avzP"

# ── Model artifacts ─────────────────────────────────────────────────
echo "[1/4] model artifacts…"
eval $RSYNC \
    "$LOCAL_PRED/src/model/model.pkl" \
    "$LOCAL_PRED/src/model/feature_names.json" \
    "$LOCAL_PRED/src/model/training_metadata.json" \
    "$USER@$IP:$REMOTE_PRED/src/model/"

# ── Baselines (training-set stats — needed for live feature scaling) ─
echo "[2/4] baselines.json…"
eval $RSYNC \
    "$LOCAL_PRED/data/processed/baselines.json" \
    "$USER@$IP:$REMOTE_PRED/data/processed/"

# ── Persistent ledgers (continuity — server picks up where local left off) ─
echo "[3/4] data ledgers…"
for f in predictions.jsonl window_pnl.jsonl arb_trades.jsonl execution_results.jsonl; do
    if [ -f "$LOCAL_PRED/data/$f" ]; then
        eval $RSYNC "$LOCAL_PRED/data/$f" "$USER@$IP:$REMOTE_PRED/data/"
    else
        echo "  skip (not present locally): $f"
    fi
done

# ── .env (Kalshi creds) ─────────────────────────────────────────────
echo "[4/4] .env files…"
if [ -f "$LOCAL_REPO/.env" ]; then
    eval $RSYNC "$LOCAL_REPO/.env" "$USER@$IP:$REMOTE_REPO/.env"
    ssh -i "$KEY" "$USER@$IP" "chmod 600 $REMOTE_REPO/.env"
else
    echo "  ✗ $LOCAL_REPO/.env not found — you must populate it on EC2 manually"
fi

echo ""
echo "Sync done. Next steps on EC2:"
echo "  ssh -i $KEY $USER@$IP"
echo "  cd ~/highlowticker-tui/btc-predictor"
echo "  .venv/bin/python scripts/run_live.py     # paper smoke test"
echo ""
echo "  When ready for live, see BTC_PREDICTOR_MIGRATION.md Phase 5."
