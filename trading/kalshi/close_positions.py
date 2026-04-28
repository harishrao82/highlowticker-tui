"""close_positions.py — flatten Kalshi crypto positions.

Modes:
  limit  : place SELL limit at (current_bid + offset) for every position
  market : aggressive sell — limit at (current_bid - 1¢) so it crosses the book

Filters:
  --coin BTC|ETH|SOL|XRP   only positions on this coin
  --side yes|no            only positions on this side
  --offset 0.05            offset above bid for limit mode (default 5¢)

Behaviors:
  --cancel-buys            cancel all open BUY orders first (default ON — avoids
                           orphaned ladder rungs sitting open while we exit)
  --no-cancel-buys         skip cancellation of buy orders
  --dry-run                print what would happen, send nothing
  --yes                    skip confirmation prompt

Examples:
  python3 close_positions.py --mode limit                 # all positions, limit @ bid+5¢
  python3 close_positions.py --mode market --yes          # flatten everything fast
  python3 close_positions.py --mode market --coin SOL     # SOL only, market
  python3 close_positions.py --mode limit --side no       # all NO positions @ bid+5¢
  python3 close_positions.py --status                     # show positions, no action
"""
import argparse
import base64
import json
import os
import sys
import time
import uuid
from collections import defaultdict

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

load_dotenv()

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

_api_key     = os.environ.get("KALSHI_API_KEY")
_secret_pem  = os.environ.get("KALSHI_API_SECRET")
if not _api_key or not _secret_pem:
    print("ERROR: KALSHI_API_KEY / KALSHI_API_SECRET not in env. Source .env first.")
    sys.exit(1)
_private_key = serialization.load_pem_private_key(_secret_pem.encode(), password=None)


def _sign(method, path):
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }

def _headers(method, path):
    return {**_sign(method, path), "Content-Type": "application/json"}


def get_positions(client, debug=False):
    """Return list of {ticker, side, position} for non-zero crypto positions."""
    path = "/trade-api/v2/portfolio/positions"
    r = client.get(KALSHI_BASE + "/portfolio/positions",
                   headers=_headers("GET", path), timeout=15)
    r.raise_for_status()
    data = r.json()

    out = []
    for p in data.get("market_positions", []):
        ticker = p.get("ticker", "")
        if not (ticker.startswith("KXBTC") or ticker.startswith("KXETH")
                or ticker.startswith("KXSOL") or ticker.startswith("KXXRP")):
            continue
        # Kalshi uses position_fp (string float). Sign convention:
        #   positive = YES shares held;  negative = NO shares held
        try:
            pos_f = float(p.get("position_fp", "0"))
        except Exception:
            pos_f = 0.0
        pos = int(round(pos_f))
        if pos == 0: continue
        out.append({
            "ticker": ticker,
            "yes_pos": pos,
            "raw":     p,
        })
    return out


def get_open_orders(client):
    """Return list of open orders (status=resting). Kalshi signs the path
    WITHOUT the query string."""
    path = "/trade-api/v2/portfolio/orders"
    r = client.get(KALSHI_BASE + "/portfolio/orders",
                   params={"status": "resting"},
                   headers=_headers("GET", path), timeout=15)
    r.raise_for_status()
    return r.json().get("orders", [])


def get_market(client, ticker, debug=False):
    """Return current bid/ask for a market."""
    path = f"/trade-api/v2/markets/{ticker}"
    r = client.get(KALSHI_BASE + f"/markets/{ticker}",
                   headers=_headers("GET", path), timeout=15)
    r.raise_for_status()
    m = r.json().get("market", {})
    if debug:
        bk = [k for k in m.keys() if any(x in k.lower() for x in ("bid","ask","price"))]
        print(f"  [debug {ticker}] price keys: {bk}")
        for k in bk:
            print(f"    {k} = {m.get(k)}")

    def _f(v):
        if v is None: return None
        try: return float(v)
        except: return None

    # Kalshi current API uses *_dollars fields (already in $).
    return {
        "yes_bid": _f(m.get("yes_bid_dollars")),
        "yes_ask": _f(m.get("yes_ask_dollars")),
        "no_bid":  _f(m.get("no_bid_dollars")),
        "no_ask":  _f(m.get("no_ask_dollars")),
        "raw": m,
    }


def cancel_order(client, order_id, dry_run=False):
    if dry_run:
        print(f"  [dry] would cancel {order_id[:8]}")
        return True
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    r = client.delete(KALSHI_BASE + f"/portfolio/orders/{order_id}",
                      headers=_headers("DELETE", path), timeout=15)
    if r.status_code in (200, 204):
        return True
    print(f"  cancel {order_id[:8]} failed: {r.status_code} {r.text[:120]}")
    return False


def place_sell(client, ticker, side, price_dollars, count, dry_run=False):
    """Place a SELL order. Kalshi expresses sell on YES side as the same.
    For NO, we must convert price."""
    if dry_run:
        print(f"  [dry] SELL {side.upper()} {count}sh @ ${price_dollars:.2f} on {ticker}")
        return None
    path = "/trade-api/v2/portfolio/orders"
    yes_price = f"{price_dollars:.2f}" if side == "yes" else f"{1.0 - price_dollars:.2f}"
    body = {
        "ticker":            ticker,
        "action":            "sell",
        "side":              side,
        "count":             count,
        "type":              "limit",
        "yes_price_dollars": yes_price,
        "client_order_id":   str(uuid.uuid4()),
    }
    r = client.post(KALSHI_BASE + "/portfolio/orders",
                    headers=_headers("POST", path),
                    content=json.dumps(body), timeout=15)
    if r.status_code in (200, 201):
        order = r.json().get("order", {})
        return order.get("order_id", "?")
    print(f"  ✗ sell {side} {count} @ {price_dollars:.2f} on {ticker} FAILED: "
          f"{r.status_code} {r.text[:200]}")
    return None


def coin_of(ticker):
    if ticker.startswith("KXBTC"): return "BTC"
    if ticker.startswith("KXETH"): return "ETH"
    if ticker.startswith("KXSOL"): return "SOL"
    if ticker.startswith("KXXRP"): return "XRP"
    return "?"


def main():
    examples = """
Examples:
  Inspection:
    close_positions.py --status                       show open positions + orders, no action

  Limit (passive — sell @ bid+offset, default offset 5¢):
    close_positions.py --mode limit                   all positions @ bid+5¢
    close_positions.py --mode limit --offset 0.03     all positions @ bid+3¢
    close_positions.py --mode limit --offset 0.10     all positions @ bid+10¢
    close_positions.py --mode limit --coin BTC        BTC only @ bid+5¢
    close_positions.py --mode limit --side yes        all YES positions @ bid+5¢
    close_positions.py --mode limit --coin SOL --side no   SOL NO positions only

  Market (aggressive — sell @ bid-1¢ to cross):
    close_positions.py --mode market --yes            flatten EVERYTHING fast (no prompt)
    close_positions.py --mode market --coin BTC --yes BTC only, fast
    close_positions.py --mode market --side yes       flatten all YES side, with prompt

  Dry-run (print plan, send nothing):
    close_positions.py --mode market --dry-run        what would market-flatten do?
    close_positions.py --mode limit --coin ETH --dry-run

  Skip the BUY-order cancellation (default ON):
    close_positions.py --mode limit --no-cancel-buys  leaves open ladder rungs alone

Notes:
  • Default behavior: cancels open BUY orders matching --coin/--side filters first,
    then places SELL orders for matching positions.
  • Confirmation prompt before sending unless --yes / -y passed.
  • Crypto-only: only KXBTC/KXETH/KXSOL/KXXRP positions are touched.
"""
    ap = argparse.ArgumentParser(
        description="Flatten Kalshi crypto positions.",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--mode", choices=["limit", "market"],
                    help="limit = sell @ bid+offset; market = sell @ bid-0.01 (cross)")
    ap.add_argument("--coin", choices=["BTC","ETH","SOL","XRP"], help="filter by coin")
    ap.add_argument("--side", choices=["yes","no"], help="filter by side")
    ap.add_argument("--offset", type=float, default=0.05, help="offset above bid for limit (default 0.05)")
    ap.add_argument("--no-cancel-buys", action="store_true",
                    help="skip cancellation of open BUY orders before exit")
    ap.add_argument("--dry-run", action="store_true", help="print plan, don't send")
    ap.add_argument("--yes", "-y", action="store_true", help="skip confirmation")
    ap.add_argument("--status", action="store_true", help="show positions only, no action")
    args = ap.parse_args()

    if not args.status and not args.mode:
        ap.error("--mode is required unless --status")

    with httpx.Client() as c:
        # Inventory
        positions = get_positions(c, debug=args.status)
        # Per-ticker per-side share count
        # KX positions return a single 'position' field per ticker which is the
        # signed YES position. To get NO, we need to look at position differently.
        # Kalshi crypto markets have separate YES and NO sides — both can be held.
        # The 'market_positions' typically returns each side as a separate entry?
        # Inspect raw to confirm:
        # If the API returns one row per ticker with signed position:
        #    pos > 0  → holding YES shares (sell yes to close)
        #    pos < 0  → holding NO shares (sell no to close — count = abs(pos))
        # If it returns one row per (ticker, side), split is direct.

        plan = []   # list of (ticker, side, shares_to_sell)
        for p in positions:
            tk = p["ticker"]; pos = p["yes_pos"]
            if args.coin and coin_of(tk) != args.coin: continue
            if pos > 0:
                if args.side and args.side != "yes": continue
                plan.append((tk, "yes", pos))
            elif pos < 0:
                if args.side and args.side != "no": continue
                plan.append((tk, "no", abs(pos)))

        if args.status:
            print(f"=== {len(plan)} open crypto position(s) ===")
            for tk, side, sh in sorted(plan):
                print(f"  {tk:35} {side:3}  {sh:>4} shares")
            print()
            # Open buy orders
            orders = get_open_orders(c)
            crypto_orders = [o for o in orders if o.get("ticker","").startswith(("KXBTC","KXETH","KXSOL","KXXRP"))]
            print(f"=== {len(crypto_orders)} open crypto order(s) ===")
            for o in crypto_orders[:30]:
                action = o.get("action","?"); side = o.get("side","?")
                px = o.get("yes_price", o.get("no_price", 0)) / 100.0
                count = o.get("count", 0); rem = o.get("remaining_count", count)
                print(f"  {o.get('ticker','?'):35} {action:4} {side:3}  "
                      f"{rem}/{count}sh @ ${px:.2f}  id={o.get('order_id','')[:8]}")
            return

        if not plan:
            print("No matching positions to close.")
            return

        # Pull market quotes for each ticker in plan
        ticker_quotes = {}
        for tk in set(p[0] for p in plan):
            try: ticker_quotes[tk] = get_market(c, tk)
            except Exception as e: print(f"  ! quote {tk}: {e}")

        # Build sell plan with prices
        print(f"=== Plan ({args.mode}) ===")
        if not args.no_cancel_buys:
            print("  • cancel all open BUY orders matching filters first")
        sells = []
        for tk, side, sh in plan:
            q = ticker_quotes.get(tk)
            if not q:
                print(f"  ! skipping {tk} (no quote)"); continue
            bid = q["yes_bid"] if side == "yes" else q["no_bid"]
            ask = q["yes_ask"] if side == "yes" else q["no_ask"]
            if bid is None or bid <= 0:
                print(f"  ! skipping {tk} {side} (no bid)"); continue
            if args.mode == "limit":
                sell_px = round(bid + args.offset, 2)
            else:  # market — sit slightly below bid to ensure fill (cross)
                sell_px = round(max(0.01, bid - 0.01), 2)
            # cap at $0.99 max
            sell_px = min(sell_px, 0.99)
            sells.append((tk, side, sh, sell_px, bid, ask))
            print(f"  {tk:35} sell {side} {sh:>3}sh @ ${sell_px:.2f}  "
                  f"(bid ${bid:.2f} / ask ${ask:.2f})")

        if not sells:
            print("Nothing to sell.")
            return

        if not args.yes and not args.dry_run:
            ans = input(f"\nProceed? [y/N]: ").strip().lower()
            if ans != "y":
                print("Aborted.")
                return

        # Cancel open buys (matching filters) first
        if not args.no_cancel_buys:
            print(f"\n=== Cancelling open BUY orders (matching filters) ===")
            orders = get_open_orders(c)
            n_cancel = 0
            for o in orders:
                if o.get("action") != "buy": continue
                tk = o.get("ticker","")
                if not tk.startswith(("KXBTC","KXETH","KXSOL","KXXRP")): continue
                if args.coin and coin_of(tk) != args.coin: continue
                if args.side and o.get("side") != args.side: continue
                oid = o.get("order_id","")
                if cancel_order(c, oid, args.dry_run):
                    n_cancel += 1
            print(f"  cancelled: {n_cancel}")

        # Place sells
        print(f"\n=== Placing SELL orders ===")
        n_ok = 0
        for tk, side, sh, px, bid, ask in sells:
            oid = place_sell(c, tk, side, px, sh, args.dry_run)
            if oid:
                print(f"  ✓ {tk} sell {side} {sh}@{px:.2f}  → {oid[:8]}")
                n_ok += 1
        print(f"\nDone: {n_ok}/{len(sells)} sell orders placed.")


if __name__ == "__main__":
    main()
