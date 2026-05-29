#!/bin/bash
# resync_from_mac.sh — push Mac's authoritative state to AWS and restart
# the service so its in-memory auto-tuner rebuilds from Mac's data.
#
# Use when you want to GUARANTEE Mac == AWS thresholds (e.g. right after a
# window settle, when you suspect tick-data divergence may have caused
# different auto-tune optima).
#
# Cost: AWS goes down for ~15 seconds during the restart. Any window-roll
# events that arrive in that gap will be picked up by the rehydrate on
# restart, so no data loss — but no live arb decisions during the gap.

set -euo pipefail

IP="${IP:-23.20.34.12}"
KEY="${KEY:-$HOME/.ssh/id_ed25519}"
USER="${EC2_USER:-ec2-user}"

LOCAL_REPO="$(cd "$(dirname "$0")/../.." && pwd)"
LOCAL_PRED="$LOCAL_REPO/btc-predictor"
REMOTE_PRED="/home/$USER/highlowticker-tui/btc-predictor"

echo "═══════════════════════════════════════════════════════════"
echo " resync_from_mac.sh — push Mac state to AWS + restart"
echo "═══════════════════════════════════════════════════════════"

# Sync the four files that determine auto-tune output:
#   1. predictions.jsonl — per-tick observations (~17 MB/day, incremental rsync)
#   2. window_pnl.jsonl  — per-window settle summaries (auto-tuner input)
#   3. arb_trades.jsonl  — per-trade ledger (continuity for the dashboard)
#   4. execution_results.jsonl — live-order paths/fees (if any)
RSYNC="rsync -e 'ssh -i $KEY -o StrictHostKeyChecking=accept-new' -avzP"
for f in predictions.jsonl window_pnl.jsonl arb_trades.jsonl execution_results.jsonl; do
    if [ -f "$LOCAL_PRED/data/$f" ]; then
        eval $RSYNC "$LOCAL_PRED/data/$f" "$USER@$IP:$REMOTE_PRED/data/"
    fi
done

# Compare thresholds BEFORE restart
echo
echo "→ thresholds BEFORE restart:"
echo "  Mac:  $(curl -s http://localhost:8000/window_pnl_log?limit=1 2>/dev/null \
            | python3 -c 'import json,sys; print(json.load(sys.stdin).get("active_thresholds"))' 2>/dev/null || echo '(unreachable)')"
echo "  AWS:  $(curl -s https://described-bras-memories-narrative.trycloudflare.com/window_pnl_log?limit=1 \
            | python3 -c 'import json,sys; print(json.load(sys.stdin).get("active_thresholds"))')"

# Restart AWS service so rehydrate runs against the freshly-synced data
echo
echo "→ restarting btc_predictor on AWS…"
ssh -i "$KEY" "$USER@$IP" 'sudo systemctl restart btc_predictor'

echo
echo "→ waiting for AWS startup…"
for i in $(seq 1 90); do
    if curl -s -m 2 -o /dev/null -w "%{http_code}" \
         https://described-bras-memories-narrative.trycloudflare.com/window_pnl_log?limit=1 \
         2>/dev/null | grep -q 200; then
        echo "  ready in ${i}s"
        break
    fi
    sleep 1
done

# Re-compare
echo
echo "→ thresholds AFTER restart:"
echo "  Mac:  $(curl -s http://localhost:8000/window_pnl_log?limit=1 2>/dev/null \
            | python3 -c 'import json,sys; print(json.load(sys.stdin).get("active_thresholds"))' 2>/dev/null || echo '(unreachable)')"
echo "  AWS:  $(curl -s https://described-bras-memories-narrative.trycloudflare.com/window_pnl_log?limit=1 \
            | python3 -c 'import json,sys; print(json.load(sys.stdin).get("active_thresholds"))')"
