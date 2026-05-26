#!/bin/bash
# bootstrap_btc_predictor.sh — one-shot AWS setup for the BTC predictor stack.
#
# Run AFTER:
#   1. SSH'd into the EC2 instance
#   2. git clone'd or git pull'd the repo to ~/highlowticker-tui
#   3. Run Phase 0 from BTC_PREDICTOR_MIGRATION.md to stop any old services
#
# Idempotent: re-running this just refreshes the venv and systemd unit.

set -euo pipefail

USER_NAME="$(whoami)"
HOME_DIR="$(eval echo ~$USER_NAME)"
REPO_DIR="$HOME_DIR/highlowticker-tui"
PRED_DIR="$REPO_DIR/btc-predictor"

if [ ! -d "$REPO_DIR" ]; then
    echo "✗ $REPO_DIR not found — git clone the repo first."
    exit 1
fi

echo "═══════════════════════════════════════════════════════════"
echo " bootstrap_btc_predictor.sh"
echo "  user:   $USER_NAME"
echo "  repo:   $REPO_DIR"
echo "  pred:   $PRED_DIR"
echo "═══════════════════════════════════════════════════════════"

# ── 1. System packages ──────────────────────────────────────────────
echo "[1/6] Installing system packages…"
if   command -v dnf >/dev/null;  then PKG=dnf
elif command -v yum >/dev/null;  then PKG=yum
elif command -v apt-get >/dev/null; then PKG=apt-get
else echo "Unsupported distro."; exit 1
fi

if [ "$PKG" = "apt-get" ]; then
    sudo apt-get update -y
    sudo apt-get install -y \
        python3.13 python3.13-venv python3.13-dev \
        build-essential git curl libffi-dev libssl-dev tmux htop rsync
else
    sudo "$PKG" install -y \
        python3.13 python3.13-devel git gcc gcc-c++ make \
        libffi-devel openssl-devel tmux htop rsync || \
    sudo "$PKG" install -y python3.13 python3.13-devel git tmux htop rsync
    # Some Amazon Linux versions need the build deps under different names —
    # the second invocation falls back to "just install what's available".
fi

PYTHON=$(command -v python3.13 || command -v python3.12 || command -v python3)
echo "  using $PYTHON ($($PYTHON --version))"

# ── 2. Virtualenv ───────────────────────────────────────────────────
echo "[2/6] Setting up .venv…"
cd "$PRED_DIR"
if [ ! -d ".venv" ]; then
    "$PYTHON" -m venv .venv
fi
.venv/bin/pip install --upgrade pip wheel
.venv/bin/pip install -r requirements.txt

# ── 3. .env stub ────────────────────────────────────────────────────
echo "[3/6] Preparing .env…"
# The Kalshi auth (loaded by trading/kalshi/kalshi_auth.py) reads from the
# REPO ROOT .env, not btc-predictor/.env. Confirm both exist; create if not.
for env in "$REPO_DIR/.env" "$PRED_DIR/.env"; do
    if [ ! -f "$env" ]; then
        cat > "$env" <<EOF
# Auto-generated stub. Edit to add real credentials before starting service.
KALSHI_API_KEY=
KALSHI_API_SECRET=
EOF
        chmod 600 "$env"
        echo "  created stub: $env  (chmod 600)"
    else
        echo "  exists:       $env"
    fi
done

# ── 4. Data dir ─────────────────────────────────────────────────────
echo "[4/6] Ensuring data/ directory exists…"
mkdir -p "$PRED_DIR/data"

# ── 5. systemd units (predictor + cloudflared tunnel) ───────────────
echo "[5/7] Installing systemd units…"
for svc in btc_predictor btc_predictor_tunnel; do
    SRC="$REPO_DIR/deploy/aws/${svc}.service"
    DST="/etc/systemd/system/${svc}.service"
    if [ ! -f "$SRC" ]; then
        echo "✗ $SRC missing"; exit 1
    fi
    sudo sed -e "s|__USER__|$USER_NAME|g" \
             -e "s|__PRED_DIR__|$PRED_DIR|g" \
             "$SRC" | sudo tee "$DST" >/dev/null
    echo "  installed → $DST"
done
sudo systemctl daemon-reload

# ── 6. cloudflared (Cloudflare Tunnel daemon) ───────────────────────
# Outbound-only tunnel that publishes localhost:8000 to a
# https://<id>.trycloudflare.com URL. No inbound port needed.
echo "[6/7] Installing cloudflared…"
if ! command -v cloudflared >/dev/null; then
    # Detect arch (t4g.* = arm64, t3.* = amd64)
    ARCH=$(uname -m)
    case "$ARCH" in
        aarch64|arm64) CF_ARCH=arm64 ;;
        x86_64)        CF_ARCH=amd64 ;;
        *) echo "✗ unsupported arch: $ARCH"; exit 1 ;;
    esac
    CF_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_ARCH}"
    sudo curl -fsSL -o /usr/local/bin/cloudflared "$CF_URL"
    sudo chmod +x /usr/local/bin/cloudflared
    echo "  installed: $(/usr/local/bin/cloudflared --version | head -1)"
else
    echo "  already present: $(cloudflared --version | head -1)"
fi

# ── 7. Caffeinate replacement (Linux equivalent is just systemd) ─────
# On macOS we run under `caffeinate -i` to prevent idle sleep. On EC2 the
# instance never sleeps, so no equivalent needed.

cat <<EOF

═══════════════════════════════════════════════════════════
 Bootstrap complete.

 Next steps:
   1. Edit $REPO_DIR/.env  and add KALSHI_API_KEY + KALSHI_API_SECRET
      (or use the sync_artifacts.sh script from your Mac to copy them)

   2. From your Mac, sync model + state files:
        bash deploy/aws/sync_artifacts.sh <PUBLIC-IP>

   3. Paper smoke test (foreground, Ctrl-C to stop):
        cd $PRED_DIR && .venv/bin/python scripts/run_live.py

   4. Once paper looks good, enable LIVE_TRADING via override:
        sudo systemctl edit btc_predictor
        # add: [Service]
        #      Environment="LIVE_TRADING=1"

   5. Start both services (predictor + Cloudflare tunnel):
        sudo systemctl enable --now btc_predictor
        sudo systemctl enable --now btc_predictor_tunnel
        journalctl -u btc_predictor -f         # in one terminal
        journalctl -u btc_predictor_tunnel -f  # in another

   6. Grab the dashboard URL (Cloudflare prints it on tunnel start):
        journalctl -u btc_predictor_tunnel | \\
          grep -oE 'https://[a-z0-9.-]*trycloudflare.com' | tail -1
      Open that URL in your browser — no SSH tunnel needed.
═══════════════════════════════════════════════════════════
EOF
