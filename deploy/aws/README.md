# AWS Migration Guide — Kalshi Arb Bot

End-to-end recipe to move `kalshi_arb_live.py` from your laptop to a cheap, fast EC2 instance in `us-east-1` (where Kalshi is hosted). Expected order-RTT improvement: ~85ms → ~15-25ms.

(`kalshi_hour_mom.py` will continue to run on your laptop — only the arb bot moves to AWS.)

---

## TL;DR

```
1. Provision  t4g.small in us-east-1, Amazon Linux 2023, 30GB EBS
2. SSH in     ssh -i <key>.pem ec2-user@<public-ip>
3. Bootstrap  REPO_URL=git@github.com:<you>/highlowticker-tui.git \
              bash <(curl -fsSL https://raw.githubusercontent.com/<you>/<repo>/main/deploy/aws/bootstrap.sh)
4. Set creds  vi ~/highlowticker-tui/.env       (paste KALSHI_API_KEY + SECRET)
5. Smoke test cd ~/highlowticker-tui && .venv/bin/python -u trading/kalshi/kalshi_arb_live.py
              (verify [kalshi] WS connected — Ctrl-C)
6. Enable     sudo systemctl enable --now kalshi_arb_live
7. Watch      tail -f /tmp/kalshi_arb.log
```

---

## Phase 1 — Provision the instance

You can use the **AWS Console** or **AWS CLI**. Console is easier first time.

### Option A — Console (recommended for first-time)

1. Go to https://console.aws.amazon.com/ec2/ and select region **us-east-1 (N. Virginia)** (top-right).
2. Click **Launch instance**.
3. Settings:
   - **Name**: `kalshi-arb-bot`
   - **AMI**: Amazon Linux 2023 (AL2023, free-tier eligible) — or Ubuntu 24.04 LTS
   - **Instance type**: `t4g.small` (ARM, $15/month) or `t3.small` (x86, $15/month). t4g is slightly cheaper and faster.
   - **Key pair**: create or pick existing. If new, download the `.pem` and `chmod 400` it.
   - **Network**: default VPC, public subnet
   - **Security group** (inbound):
     - SSH (22) from **My IP** only
     - Nothing else
   - **Storage**: 30 GB gp3 (default 8 GB is tight)
   - **Advanced** → leave most defaults; under "User data" you can paste this for auto-bootstrap, but easier to do it after SSH
4. **Launch**

After a minute, copy the **Public IPv4 address** from the instance details.

### Option B — AWS CLI (if you have it set up)

```bash
# install on your Mac if not already:
brew install awscli
aws configure   # paste your access key, secret, default region us-east-1

# create key pair if needed
aws ec2 create-key-pair --key-name kalshi-bot --query 'KeyMaterial' --output text > ~/.ssh/kalshi-bot.pem
chmod 400 ~/.ssh/kalshi-bot.pem

# get latest Amazon Linux 2023 AMI ID (us-east-1)
AMI=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-arm64" \
  --query "Images | sort_by(@, &CreationDate)[-1].ImageId" --output text)

# launch
aws ec2 run-instances \
  --image-id $AMI \
  --instance-type t4g.small \
  --key-name kalshi-bot \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kalshi-arb-bot}]'
```

---

## Phase 2 — SSH + bootstrap

```bash
# from your Mac
ssh -i ~/.ssh/kalshi-bot.pem ec2-user@<PUBLIC-IP>
```

Once in:

```bash
# tell bootstrap where the code is (private repo? need a deploy key first — see appendix)
export REPO_URL=git@github.com:harishrao82/highlowticker-tui.git

# pull and run the bootstrap script
curl -fsSL https://raw.githubusercontent.com/harishrao82/highlowticker-tui/main/deploy/aws/bootstrap.sh | bash
```

If your repo is **private**, the curl will fail (no auth). Two workarounds:
- Option 1: clone manually with a deploy key (see Appendix), then `bash deploy/aws/bootstrap.sh`
- Option 2: scp the `bootstrap.sh` from your laptop, then run it

```bash
# from your Mac (alt path)
scp -i ~/.ssh/kalshi-bot.pem deploy/aws/bootstrap.sh ec2-user@<PUBLIC-IP>:/tmp/
ssh -i ~/.ssh/kalshi-bot.pem ec2-user@<PUBLIC-IP>
# then on the server:
export REPO_URL=git@github.com:harishrao82/highlowticker-tui.git
bash /tmp/bootstrap.sh
```

The script:
1. Installs Python 3.13, git, build tools
2. Clones the repo to `~/highlowticker-tui`
3. Creates `.venv` with `httpx`, `websockets`, `cryptography`, `python-dotenv`, `pandas`
4. Writes a stub `.env` (you fill in)
5. Installs **systemd units** for both bots — enabled but NOT started

---

## Phase 3 — Configure secrets

```bash
vi ~/highlowticker-tui/.env
```

Paste your `KALSHI_API_KEY` and `KALSHI_API_SECRET` (the multiline private key). Save.

```bash
chmod 600 ~/highlowticker-tui/.env   # already done by bootstrap, double-check
```

**Tip:** to copy from your laptop's existing `.env`, use:
```bash
# from your Mac
scp -i ~/.ssh/kalshi-bot.pem .env ec2-user@<PUBLIC-IP>:~/highlowticker-tui/.env
```

---

## Phase 4 — Smoke test

```bash
cd ~/highlowticker-tui
.venv/bin/python -u trading/kalshi/kalshi_arb_live.py
```

Look for:
```
kalshi_arb_live starting...
  coins:           BTC, ETH, SOL, XRP
  ...
[kalshi] WS connected (handshake XXms)    ← ★ this should be <50ms on AWS!
[kalshi] WS subscribed 4 tickers ...
```

If `WS connected (handshake)` is well under 50ms vs your laptop's ~157ms, the AWS network advantage is real. Ctrl-C to stop.

---

## Phase 5 — Enable as a service

```bash
sudo systemctl enable --now kalshi_arb_live
sudo systemctl status kalshi_arb_live

# logs
tail -f /tmp/kalshi_arb.log
```

systemd will:
- Auto-start on boot
- Auto-restart on crash (after 5s)
- Log to /tmp/kalshi_*.log AND to journal (use `journalctl -u kalshi_arb_live -f`)

---

## Phase 6 — Cutover from laptop

Once you're confident the AWS bot is running cleanly (24-hour shakedown is reasonable), kill the laptop bot:

```bash
# on your Mac
pgrep -f kalshi_arb_live | xargs kill
```

If you want a clean A/B test BEFORE killing, you can run BOTH for a few hours — but make sure they're using the SAME Kalshi account and you understand they'll fire on the same triggers (= 2x position). Generally simpler to cutover and observe.

---

## Operations cheat sheet

```bash
# Status
sudo systemctl status kalshi_arb_live

# Restart (e.g., after pulling new code)
sudo systemctl restart kalshi_arb_live

# Stop
sudo systemctl stop kalshi_arb_live

# Disable (don't auto-start on boot)
sudo systemctl disable kalshi_arb_live

# Logs (live)
tail -f /tmp/kalshi_arb.log
sudo journalctl -u kalshi_arb_live -f

# Pull new code from your Mac
ssh ec2-user@<IP>
cd ~/highlowticker-tui && git pull
sudo systemctl restart kalshi_arb_live

# Read trade log on laptop without SSH'ing
ssh ec2-user@<IP> 'tail -50 ~/.kalshi_arb_trades.jsonl' | python3 -m json.tool

# Bring trade logs back to laptop for analysis
scp ec2-user@<IP>:~/.kalshi_arb_trades.jsonl ~/.kalshi_arb_trades_aws.jsonl
```

---

## What to watch in the first 24 hours

### Latency improvement
Expected on AWS:
- WS handshake: 5-30ms (vs 157ms local)
- Order RTT: 15-25ms (vs 80-90ms local)
- Tick→trade: 50-100ms (vs 200-1700ms local)

### Fill rate improvement
Local NOFILL rate was ~28% (35 of 127 fires). On AWS, expect ~10-15% — the slippage buffer + fast RTT catches more asks before they move.

### A red flag to investigate
If WS handshake / order RTT are NOT lower than local, something is wrong. Likely:
- Wrong region (us-west adds 60ms)
- DNS resolving to non-AWS Kalshi endpoints
- Bot running from a non-AWS network (verify with `curl ifconfig.me`)

---

## Cost monitoring

```
EC2 t4g.small    ~$15/month   (ARM, slightly cheaper than t3.small)
EBS 30GB gp3     ~$2.40/month
Data transfer    ~$0-2/month
─────────────────────────
TOTAL            ~$17-20/month
```

Add a billing alert at $25/month: AWS Console → Billing → Budgets → Create budget → $25/month.

---

## Appendix: Setting up a deploy key for private repos

```bash
# on the EC2 instance
ssh-keygen -t ed25519 -C "ec2-kalshi-bot" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
# Copy that public key — paste it into:
#   GitHub → your repo → Settings → Deploy keys → Add deploy key
#   Name: aws-ec2-kalshi-bot
#   Allow write access: NO (read-only is enough)

# verify
ssh -T git@github.com   # should say "Hi <user>! You've successfully authenticated"
```

---

## Appendix: Time sync

Your bot signs Kalshi requests with timestamps. AWS EC2 instances should already have NTP, but verify:

```bash
timedatectl status
# Look for "System clock synchronized: yes" and "NTP service: active"
```

Drift of >1 second can cause `EXPIRED_TIMESTAMP` errors (the bug we already saw locally).

---

## Appendix: Optional — TUI access via tmux

```bash
# on EC2
tmux new -s bot
# (run things, ctrl-b d to detach)
tmux attach -t bot
```

Or just use `sudo journalctl -u kalshi_arb_live -f` — cleaner.
