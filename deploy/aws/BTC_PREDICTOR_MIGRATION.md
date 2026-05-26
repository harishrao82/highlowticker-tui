# BTC Predictor — AWS Migration Playbook

End-to-end recipe to move the **BTC 15-min predictor + n-5 arb executor** stack from local Mac to AWS EC2 in `us-east-1` (where Kalshi is hosted). Expected order-RTT improvement: ~85ms → ~15-25ms.

This **replaces** the older `kalshi_arb_live` bot. Phase 0 stops everything currently running so we don't double-trade.

Dashboard access is via **Cloudflare Tunnel** (`cloudflared`): the EC2 server only exposes port 8000 on localhost, `cloudflared` punches an outbound connection to Cloudflare, and you reach the dashboard via a public `https://<id>.trycloudflare.com` URL (or a stable `<sub>.<your-domain>` if you wire it to your Cloudflare account). No inbound ports open on the security group.

> **Live-trading flag** ships disabled by default. After bootstrap you smoke-test in paper mode, validate fills, *then* flip `LIVE_TRADING=1`.

---

## TL;DR

```bash
# On the EC2 instance, after SSH'ing in:
sudo systemctl stop kalshi_arb_live      # ← Phase 0: stop the old bot
sudo systemctl disable kalshi_arb_live

cd ~/highlowticker-tui && git pull
bash deploy/aws/bootstrap_btc_predictor.sh

# From your Mac (in another terminal):
bash deploy/aws/sync_artifacts.sh <PUBLIC-IP>     # ships model + JSONL state

# Back on EC2:
vi ~/highlowticker-tui/btc-predictor/.env         # paste Kalshi creds if missing
sudo systemctl enable --now btc_predictor
sudo systemctl enable --now btc_predictor_tunnel  # cloudflared tunnel to dashboard

# Read the URL the tunnel announced — paste in your browser, done.
journalctl -u btc_predictor_tunnel -n 50 | grep trycloudflare.com
```

After 24h paper smoke-test:

```bash
# Edit the systemd unit's Environment line to add LIVE_TRADING=1
sudo systemctl edit btc_predictor       # adds [Service] Environment="LIVE_TRADING=1"
sudo systemctl restart btc_predictor
```

---

## Phase 0 — Stop everything currently running on AWS

If your EC2 instance is still running the old `kalshi_arb_live` setup (per `deploy/aws/README.md`), kill it cleanly:

```bash
ssh -i ~/.ssh/kalshi-bot.pem ec2-user@<EXISTING-IP>

# Stop the systemd services
sudo systemctl stop kalshi_arb_live 2>/dev/null
sudo systemctl stop kalshi_hour_mom 2>/dev/null
sudo systemctl disable kalshi_arb_live 2>/dev/null
sudo systemctl disable kalshi_hour_mom 2>/dev/null

# Kill any straggler Python processes
pkill -f "kalshi_arb_live\|kalshi_hour_mom" 2>/dev/null || true

# Confirm clean
ps aux | grep -i kalshi | grep -v grep
sudo systemctl list-units --type=service --state=running | grep -i kalshi
```

If you want a completely fresh instance instead of reusing this one, see Phase 1 below. If reusing, jump to Phase 2 (the existing instance already has Python + git installed).

**Stop the local Mac server too**, otherwise you'll have two systems placing orders on the same account:

```bash
# On your Mac
lsof -i:8000 -sTCP:LISTEN -t | xargs kill   # stops the FastAPI server
```

---

## Phase 1 — Provision EC2 (only if starting fresh)

### Recommended sizing

| spec | value | why |
|---|---|---|
| **Region** | `us-east-1` | Kalshi is hosted here; ~15ms order RTT |
| **Instance** | `t4g.medium` (2 vCPU, 4 GB) ARM | LightGBM + pandas + websockets fit in ~1.5 GB; rehydrate walk-forward grids need the second vCPU |
| **AMI** | Amazon Linux 2023 (`arm64`) | Matches t4g.* arch |
| **EBS** | 30 GB gp3 | predictions.jsonl grows ~17 MB/day → 6 GB/year |
| **Security group inbound** | SSH (22) from My IP only | Dashboard is reached via outbound-only Cloudflare tunnel; no inbound :8000 needed |
| **Outbound** | all open | Kalshi REST + WS, Coinbase WS, GitHub |
| **Cost** | ~$30/mo instance + ~$2.40/mo EBS = **~$33/mo** | (vs $17/mo for t4g.small — worth the headroom for safety) |

### Console launch (5 minutes)

1. https://console.aws.amazon.com/ec2/ → region **us-east-1**
2. **Launch instance**
3. Name: `btc-predictor`
4. AMI: Amazon Linux 2023 (AL2023) — make sure **arm64** is selected
5. Instance type: **t4g.medium**
6. Key pair: pick existing or create new (download `.pem`, `chmod 400 ~/.ssh/<key>.pem`)
7. Security group: SSH (22) from **My IP**
8. Storage: **30 GiB gp3**
9. Launch

Copy the **Public IPv4** when ready.

### CLI launch (if you prefer)

```bash
AMI=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-arm64" \
  --query "Images | sort_by(@, &CreationDate)[-1].ImageId" --output text)

aws ec2 run-instances \
  --image-id "$AMI" \
  --instance-type t4g.medium \
  --key-name <your-key> \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=btc-predictor}]'
```

---

## Phase 2 — Bootstrap

SSH in and run the predictor-specific bootstrap script:

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<PUBLIC-IP>

# If this is a fresh instance, clone first:
git clone git@github.com:harishrao82/highlowticker-tui.git
# (or curl-pull a deploy script — see appendix on deploy keys)

cd ~/highlowticker-tui
git pull
bash deploy/aws/bootstrap_btc_predictor.sh
```

The script:

1. Installs Python 3.13, git, build tools (or skips if already installed)
2. Creates `btc-predictor/.venv` and installs `btc-predictor/requirements.txt`
3. Installs the `btc-predictor/.env` stub if missing (you fill in `KALSHI_API_KEY`/`KALSHI_API_SECRET`)
4. Copies `deploy/aws/btc_predictor.service` into `/etc/systemd/system/`
5. Reloads systemd — **does not start the service yet** (you sync artifacts first)

---

## Phase 3 — Sync model + persistent state from your Mac

The model artifact (~1.5 MB) and the persistent ledgers (`window_pnl.jsonl`, `arb_trades.jsonl`, `execution_results.jsonl`, `predictions.jsonl`) need to ride along so the system picks up exactly where local left off:

```bash
# From your Mac
bash deploy/aws/sync_artifacts.sh <PUBLIC-IP>
```

This rsyncs:
- `btc-predictor/src/model/model.pkl` + `feature_names.json` + `training_metadata.json`
- `btc-predictor/data/processed/baselines.json`
- `btc-predictor/data/window_pnl.jsonl` (continuity for auto-tuner)
- `btc-predictor/data/arb_trades.jsonl` (per-trade ledger)
- `btc-predictor/data/execution_results.jsonl` (live order history)
- `btc-predictor/data/predictions.jsonl` (chart history)
- The parent repo's `.env` if you don't want to retype Kalshi creds

The script is idempotent — re-running it just refreshes anything that changed.

---

## Phase 4 — Smoke test in paper mode

```bash
# On EC2
cd ~/highlowticker-tui/btc-predictor
.venv/bin/python scripts/run_live.py
```

Look for:

```
[rehydrate] Restored 20 settled-window rows (20 LIVE from window_pnl.jsonl, ...
[startup] LIVE_TRADING not set — paper mode only
INFO:     Application startup complete.
```

In another terminal on the EC2 instance, start the Cloudflare tunnel
(also installed by the bootstrap script):

```bash
cloudflared tunnel --url http://localhost:8000
```

Within a few seconds it prints a public URL like
`https://random-words-xxxx.trycloudflare.com`. Open that in your browser.
No SSH tunnel required, no inbound ports opened on AWS.

The dashboard should show the same windows you had locally. Watch for ~15 minutes — settled-window rows should populate as new windows close. **Stay in paper mode for the first 24 hours** so you can verify the new environment matches local behavior.

The free `trycloudflare.com` URL is **ephemeral** — it changes each time `cloudflared` restarts. For a stable URL (e.g. `dashboard.<your-domain>`), see the Cloudflare appendix at the bottom of this doc.

When done, `Ctrl-C` the foreground server. The systemd service hasn't been started yet, so this won't restart automatically.

---

## Phase 5 — Enable LIVE_TRADING

After paper-mode smoke test looks good:

```bash
sudo systemctl edit btc_predictor
```

Add to the override file that opens:

```ini
[Service]
Environment="LIVE_TRADING=1"
```

Save (`:wq`), then start:

```bash
sudo systemctl enable --now btc_predictor
sudo systemctl status btc_predictor
journalctl -u btc_predictor -f
```

The startup log should now read:

```
[startup] LIVE_TRADING=1 — Kalshi order client initialized
```

From this point on, real orders are being placed on Kalshi. The first arb fire will show:

```
[arb@T<sec>] maker POST KXBTC15M-... yes @ $0.30 (TTL 3s)
[arb@T<sec>] maker FILLED @ $0.3000  fee $0.0046  status=executed
```

(or `mkt_after_maker`, or `drop_cap_exceeded`).

---

## Operations cheat sheet

```bash
# ── Service control ────────────────────────────────────────────────
sudo systemctl status btc_predictor              # is it up?
sudo systemctl restart btc_predictor             # apply code/config change
sudo systemctl stop btc_predictor                # graceful stop

# ── Logs ──────────────────────────────────────────────────────────
journalctl -u btc_predictor -f                   # follow live
journalctl -u btc_predictor --since "1 hour ago" # recent
grep -E 'AUTOTUNE|WINDOW.*settled|\[arb@' \
  <(journalctl -u btc_predictor --since today)   # decisions only

# ── Emergency stop (any in-flight orders complete) ────────────────
touch /tmp/btc_predictor_stop                    # blocks new orders
rm /tmp/btc_predictor_stop                       # resume

# ── Dashboard URL (Cloudflare tunnel) ─────────────────────────────
# The systemd btc_predictor_tunnel service runs cloudflared and prints
# the public URL in its journal. The URL changes on restart unless you
# use a named tunnel (see appendix).
journalctl -u btc_predictor_tunnel | grep -oE 'https://[a-z0-9.-]*trycloudflare.com' | tail -1
sudo systemctl restart btc_predictor_tunnel   # → new URL

# ── Pull new code (zero-downtime mostly) ──────────────────────────
ssh ec2-user@<IP>
cd ~/highlowticker-tui && git pull
sudo systemctl restart btc_predictor

# ── Pull trade ledgers back to your Mac for analysis ──────────────
scp ec2-user@<IP>:~/highlowticker-tui/btc-predictor/data/arb_trades.jsonl \
    ./arb_trades.aws.jsonl
scp ec2-user@<IP>:~/highlowticker-tui/btc-predictor/data/execution_results.jsonl \
    ./execution_results.aws.jsonl

# ── Inspect live state directly on the box (no tunnel needed) ─────
curl -s http://localhost:8000/window_pnl_log?limit=10 | python3 -m json.tool
curl -s http://localhost:8000/arb_window | python3 -m json.tool
# Or hit the public Cloudflare URL from your Mac for the same data.
```

---

## What to watch in the first 48 hours

| metric | target | how to read |
|---|---|---|
| WS handshake latency | <50ms | `journalctl` after kalshi WS reconnects — handshake ms is logged |
| Trade roundtrip (POST → status=200) | <100ms | look at `[arb@T...] maker POST` → `[arb@T...] maker FILLED/expired` delta |
| Maker fill rate | 70-85% | `grep 'execution_path' execution_results.jsonl | sort | uniq -c` |
| Daily P&L vs local backtest | within 5% | compare AWS `window_pnl.jsonl` to local backtest from before migration |
| `outcome flip` warnings | 0-3 per day | normal — sub-1-bps moves where Kalshi BRTI ≠ our TWAP |
| Memory | <2 GB | `free -h` |
| Disk growth | <20 MB/day | `du -sh btc-predictor/data/` |

Red flags:

- WS handshake >100ms → wrong region or DNS resolving to non-AWS endpoint
- Maker fill rate <40% → book moving too fast, consider bumping offset to 1¢
- Memory creep → leak; restart and investigate
- Anything in `journalctl -u btc_predictor -p err` → real error, look at it

---

## Cost

```
EC2 t4g.medium (2 vCPU, 4 GB)    ~$30/mo
EBS 30GB gp3                     ~$2.40/mo
Data transfer (egress)           ~$1-3/mo
────────────────────────────────
TOTAL                            ~$33-35/mo
```

Set a billing alert at $45/mo: AWS Console → Billing → Budgets → Create budget → $45/month.

---

## Rollback

If anything goes catastrophically wrong:

1. **Immediate**: `touch /tmp/btc_predictor_stop` on EC2 — halts new orders. Existing positions ride to settle.
2. **Service stop**: `sudo systemctl stop btc_predictor`
3. **Revert to local**: on your Mac, `cd ~/highlowticker-tui/btc-predictor && python scripts/run_live.py` resumes locally.
4. **Terminate instance** (only if you're sure): AWS Console → Instances → terminate.

The ledger files (`arb_trades.jsonl`, `window_pnl.jsonl`) on EC2 are the authoritative record of what AWS did — pull them back before terminating.

---

## Architecture summary

```
                                       ┌──────────────────────────┐
                                       │ Coinbase Advanced Trade  │
                                       │ market_trades WS         │
                                       └────────────┬─────────────┘
                                                    │  tick stream
                                                    ▼
        EC2 t4g.medium (us-east-1)        ┌──────────────────────┐
        ─────────────────────────         │ LiveFeatureEngine    │
        systemd: btc_predictor.service    │  29 features         │
                                          │  LightGBM model      │
                                          └────────────┬─────────┘
                                                       │
                                          ┌────────────▼─────────┐    ┌────────────────┐
                                          │ prediction_publisher │◄───┤ Kalshi WS      │
                                          │  5 Hz                │    │ orderbook deltas│
                                          └────────────┬─────────┘    └────────────────┘
                                                       │
                              ┌────────────────────────┴─────────────┐
                              │                                       │
                              ▼                                       ▼
                  ┌────────────────────────┐              ┌────────────────────────┐
                  │ ArbTradeTracker        │              │ FastAPI WebSocket      │
                  │  throttle + paper P&L  │              │  /ws/predictions       │
                  │  fires async order ──┐ │              └──────────┬─────────────┘
                  └──────────────────────┼─┘                         │
                                         │                            │  WS to subscribers
                                         ▼                            │
                              ┌────────────────────────┐               │
                              │ kalshi_executor        │               ▼
                              │  maker @ bid (3s)      │     ┌────────────────────┐
                              │  → market @ ask+5¢ cap │     │ cloudflared (sibling│
                              │  → drop                │     │  systemd unit) —    │
                              └──────────┬─────────────┘     │  outbound only to   │
                                         │                    │  Cloudflare edge    │
                                         │ REST POST/GET     └──────────┬─────────┘
                                         ▼                              │
                              ┌────────────────────────┐                ▼
                              │ Kalshi API             │   https://<id>.trycloudflare.com
                              │ /portfolio/orders      │                │
                              └────────────────────────┘                ▼
                                                              Your browser, anywhere
                                                              (no inbound :8000 on AWS SG)
```

Persistent state (on EC2 disk, included in nightly EBS snapshots if you enable them):

- `btc-predictor/data/predictions.jsonl` — per-second snapshots
- `btc-predictor/data/window_pnl.jsonl` — per-window settle summary
- `btc-predictor/data/arb_trades.jsonl` — per-trade ledger
- `btc-predictor/data/execution_results.jsonl` — every live order's path/fill

---

## Appendix: GitHub deploy key (for private repos)

If your repo is private and you don't want to copy SSH keys around:

```bash
# On the EC2 instance
ssh-keygen -t ed25519 -C "ec2-btc-predictor" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
# → paste in GitHub → repo → Settings → Deploy keys → Add deploy key
#   Name: aws-btc-predictor
#   Read-only access (no need for write)

ssh -T git@github.com   # verify ("Hi <user>! You've successfully authenticated...")
```

---

## Appendix: Time sync

Kalshi rejects requests with timestamps that drift more than ~1 s. AL2023 + chrony handles this automatically, but verify:

```bash
timedatectl status
# Look for "System clock synchronized: yes" and "NTP service: active"
```

---

## Appendix: Cloudflare Tunnel — ephemeral vs named (stable URL)

The bootstrap installs `cloudflared` and a sibling systemd unit
(`btc_predictor_tunnel.service`) that runs:

```bash
cloudflared tunnel --url http://localhost:8000
```

This is the **quick / free / ephemeral** mode: every restart prints a fresh
`https://<random>.trycloudflare.com` URL with no Cloudflare account required.
Good enough for shadow + early live mode. Find the current URL with:

```bash
journalctl -u btc_predictor_tunnel | grep -oE 'https://[a-z0-9.-]*trycloudflare.com' | tail -1
```

### Upgrading to a stable, branded URL

When you want a permanent URL like `https://dashboard.<your-domain>`:

1. **Login** (one-time, opens a browser; redirects can be handled via the
   `cloudflared tunnel login` token if you don't have a GUI):

   ```bash
   cloudflared tunnel login
   ```

2. **Create a named tunnel** (replaces the random ID with a friendly name
   you'll see in the Cloudflare Zero Trust dashboard):

   ```bash
   cloudflared tunnel create btc-predictor
   ```

   This writes `~/.cloudflared/<tunnel-id>.json` (private key + cert).

3. **Create a DNS record** in your zone (run from the EC2 box; needs the
   account credential from step 1):

   ```bash
   cloudflared tunnel route dns btc-predictor dashboard.<your-domain>.com
   ```

4. **Write a tunnel config**:

   ```bash
   sudo tee ~/.cloudflared/config.yml <<EOF
   tunnel: btc-predictor
   credentials-file: /home/ec2-user/.cloudflared/<tunnel-id>.json

   ingress:
     - hostname: dashboard.<your-domain>.com
       service: http://localhost:8000
     - service: http_status:404
   EOF
   ```

5. **Switch the systemd unit** to run the named tunnel instead of the
   ephemeral one:

   ```bash
   sudo systemctl edit btc_predictor_tunnel
   # Replace ExecStart with:
   # ExecStart=/usr/local/bin/cloudflared tunnel run btc-predictor

   sudo systemctl daemon-reload
   sudo systemctl restart btc_predictor_tunnel
   ```

6. **(Optional)** Put Cloudflare Access in front of `dashboard.<your-domain>.com`
   for email/2FA-based auth — zero-trust gating without exposing your IP or
   running a separate web auth layer. Configure in Cloudflare Zero Trust →
   Access → Applications.

The ephemeral mode is recommended for the first week. Switch to a named
tunnel once the system has proven stable and you want a URL you can
bookmark / share.
