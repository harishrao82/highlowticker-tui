#!/bin/bash
# bootstrap.sh — one-shot setup for a fresh Amazon Linux 2023 (or Ubuntu 24)
# instance to run kalshi_arb_live.
#
# Usage on the EC2 instance (after SSH'ing in):
#   curl -fsSL https://raw.githubusercontent.com/<your-repo>/main/deploy/aws/bootstrap.sh | bash
# Or, after `git clone`ing the repo manually:
#   bash deploy/aws/bootstrap.sh
#
# Prereqs:
#   - SSH'd into the EC2 instance as ec2-user (Amazon Linux) or ubuntu (Ubuntu)
#   - Outbound HTTPS to api.elections.kalshi.com, polymarket.com, github.com
#
# What it does:
#   1. Installs Python 3.13 + git + build deps
#   2. Clones the repo to /home/<user>/highlowticker-tui
#   3. Creates the .venv and installs requirements
#   4. Prompts you to populate .env with KALSHI_API_KEY / KALSHI_API_SECRET
#   5. Installs systemd unit files for both bots (manual start)

set -euo pipefail

# ── Detect distribution ─────────────────────────────────────────────────
if   command -v dnf >/dev/null;  then PKG=dnf
elif command -v yum >/dev/null;  then PKG=yum
elif command -v apt-get >/dev/null; then PKG=apt-get
else echo "Unsupported distro — install Python 3.13, git, build tools manually."; exit 1
fi

USER_NAME="$(whoami)"
HOME_DIR="$(eval echo ~$USER_NAME)"
REPO_DIR="$HOME_DIR/highlowticker-tui"

echo "=========================================================="
echo " bootstrap.sh — Kalshi arb bot AWS setup"
echo "  user:   $USER_NAME"
echo "  home:   $HOME_DIR"
echo "  repo:   $REPO_DIR"
echo "  pkgmgr: $PKG"
echo "=========================================================="
echo ""

# ── 1. System packages ─────────────────────────────────────────────────
echo "[1/5] Installing system packages..."
if [ "$PKG" = "apt-get" ]; then
    sudo apt-get update -y
    sudo apt-get install -y \
        python3.13 python3.13-venv python3.13-dev \
        build-essential git curl libffi-dev libssl-dev tmux htop
else
    sudo $PKG update -y
    sudo $PKG install -y python3.13 python3.13-pip git gcc make tmux htop \
                          openssl-devel bzip2-devel libffi-devel || \
    sudo $PKG install -y python3 python3-pip git gcc make tmux htop \
                          openssl-devel bzip2-devel libffi-devel
fi
echo "  ✓ packages installed"
echo ""

# ── 2. Clone repo (if not already) ─────────────────────────────────────
if [ -d "$REPO_DIR/.git" ]; then
    echo "[2/5] Repo exists at $REPO_DIR — pulling latest..."
    cd "$REPO_DIR"
    git pull --ff-only || echo "  (note: pull failed; continuing with current checkout)"
else
    echo "[2/5] Cloning repo..."
    if [ -z "${REPO_URL:-}" ]; then
        echo "  ERROR: set REPO_URL env var first, e.g.:"
        echo "    export REPO_URL=git@github.com:harishrao82/highlowticker-tui.git"
        echo "    bash bootstrap.sh"
        exit 1
    fi
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
fi
echo "  ✓ repo at $REPO_DIR"
echo ""

# ── 3. Python venv + deps ──────────────────────────────────────────────
echo "[3/5] Creating .venv and installing requirements..."
cd "$REPO_DIR"
if [ ! -d ".venv" ]; then
    python3.13 -m venv .venv 2>/dev/null || python3 -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip wheel
# Core deps for arb/hour_mom
pip install httpx websockets cryptography python-dotenv pandas
# Convenience for status_loop, k_positions etc.
pip install rich || true
echo "  ✓ venv populated at $REPO_DIR/.venv"
echo ""

# ── 4. .env file ───────────────────────────────────────────────────────
ENV_FILE="$REPO_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "[4/5] Creating .env template at $ENV_FILE"
    cat > "$ENV_FILE" <<'EOF'
# Kalshi credentials — fill in before starting bots
KALSHI_API_KEY=
KALSHI_API_SECRET="-----BEGIN PRIVATE KEY-----
...paste your private key here...
-----END PRIVATE KEY-----"

# Optional — Polymarket auth keys for future order placement (not used by arb_live or hour_mom yet)
POLY_PRIVATE_KEY=
POLY_API_KEY=
POLY_API_SECRET=
POLY_API_PASSPHRASE=
EOF
    chmod 600 "$ENV_FILE"
    echo "  ✓ .env template written — edit it before starting bots:"
    echo "      vi $ENV_FILE"
else
    echo "[4/5] .env already exists (preserved)"
fi
echo ""

# ── 5. systemd unit file ──────────────────────────────────────────────
echo "[5/5] Installing systemd service for kalshi_arb_live..."
SVC_DIR="/etc/systemd/system"
TMP_ARB="/tmp/kalshi_arb_live.service"

cat > "$TMP_ARB" <<EOF
[Unit]
Description=Kalshi PM-vs-KS arbitrage bot (live)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$REPO_DIR
ExecStart=$REPO_DIR/.venv/bin/python -u trading/kalshi/kalshi_arb_live.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/tmp/kalshi_arb.log
StandardError=append:/tmp/kalshi_arb.log
EnvironmentFile=$REPO_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

sudo mv "$TMP_ARB" "$SVC_DIR/kalshi_arb_live.service"
sudo systemctl daemon-reload
echo "  ✓ service installed (NOT yet started)"
echo ""

cat <<EOF
==========================================================
 BOOTSTRAP DONE.

 Next steps:
   1. Edit credentials:   vi $ENV_FILE
   2. Test arb manually:  cd $REPO_DIR && .venv/bin/python -u trading/kalshi/kalshi_arb_live.py
                          (Ctrl-C after a few seconds once you see WS connect)
   3. Enable + start:     sudo systemctl enable --now kalshi_arb_live
   4. Watch logs:         tail -f /tmp/kalshi_arb.log
   5. Check status:       sudo systemctl status kalshi_arb_live

 Useful commands:
   sudo systemctl restart kalshi_arb_live    # bounce
   sudo systemctl stop    kalshi_arb_live    # halt
   sudo journalctl -u kalshi_arb_live -f     # systemd's view
==========================================================
EOF
