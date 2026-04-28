#!/usr/bin/env python3
"""
scallops_ml_dataset.py — build a supervised training dataset for Scallops' BTC decisions.

For each 10-second window (0, 10, 20, ..., 890 seconds into window), compute:
  FEATURES (snapshot at start of 10s window):
    - elapsed_sec
    - delta_pct for BTC, ETH, SOL, XRP (4 features)
    - bid/ask for Up + Down on all 4 coins (4 × 4 = 16 features)
    - BTC running state: up_shares, dn_shares, up_avg, dn_avg, total_cost
    - if_up_pnl (payout if BTC settles Up given current position)
    - if_dn_pnl (payout if BTC settles Down)
  TARGETS (what Scallops did in the NEXT 10 seconds on BTC):
    - action: 'none' / 'buy_up' / 'buy_down' / 'buy_both'
    - up_size ($)
    - dn_size ($)

Output: ~/.scallops_ml_btc.csv

Usage: python3 scallops_ml_dataset.py
"""
import json
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

BOOK_LOG = Path.home() / ".poly_book_snapshots.jsonl"
TRADE_LOG = Path.home() / ".scallops_live_trades.jsonl"
SNAPSHOT_LOG = Path.home() / ".kalshi_status_snapshots.jsonl"
DB = Path.home() / ".btc_windows.db"
OUT_CSV = Path.home() / ".scallops_ml_btc.csv"

ET = timezone(timedelta(hours=-4))

COINS = ["BTC", "ETH", "SOL", "XRP"]


def load_book():
    """book[(wts, coin, outcome)] = sorted list of (sec, best_bid, best_ask)"""
    book = defaultdict(list)
    if not BOOK_LOG.exists():
        return book
    with open(BOOK_LOG) as f:
        for line in f:
            try:
                e = json.loads(line)
                key = (e["window_start_ts"], e["coin"], e["outcome"])
                book[key].append((int(e.get("elapsed", 0)), e.get("best_bid"), e.get("best_ask")))
            except Exception:
                continue
    for k in book:
        book[k].sort()
    return book


def load_trades():
    """Scallops BTC 15-min market trades only (filter 5m / 4h / other out)."""
    trades = []
    if not TRADE_LOG.exists():
        return trades
    with open(TRADE_LOG) as f:
        for line in f:
            try:
                t = json.loads(line)
                if t.get("side") != "BUY" or t.get("coin") != "BTC":
                    continue
                slug = t.get("slug", "")
                if "-15m-" not in slug:
                    continue
                trades.append(t)
            except Exception:
                continue
    return trades


def load_deltas():
    """Coinbase-based deltas from Kalshi snapshot log, per (wts, coin) sorted."""
    d = defaultdict(list)
    if not SNAPSHOT_LOG.exists():
        return d
    with open(SNAPSHOT_LOG) as f:
        for line in f:
            try:
                s = json.loads(line)
                ts = s.get("ts"); coin = s.get("coin"); dp = s.get("delta_pct")
                elapsed = s.get("elapsed")
                if ts is None or coin is None or dp is None or elapsed is None: continue
                wts = (int(ts - elapsed) // 900) * 900
                d[(wts, coin)].append((int(elapsed), float(dp)))
            except Exception:
                continue
    for k in d: d[k].sort()
    return d


def interp(pts, sec):
    if not pts: return None
    if sec <= pts[0][0]: return pts[0][1]
    if sec >= pts[-1][0]: return pts[-1][1]
    for i in range(1, len(pts)):
        if pts[i][0] >= sec:
            s0, v0 = pts[i-1]; s1, v1 = pts[i]
            if s1 == s0: return v1
            return v0 + (sec - s0) / (s1 - s0) * (v1 - v0)
    return pts[-1][1]


def book_at(book, wts, coin, outcome, sec):
    """Forward-fill Polymarket book: return (best_bid, best_ask) at or before sec."""
    pts = book.get((wts, coin, outcome))
    if not pts: return (None, None)
    best_bid = best_ask = None
    for s, bid, ask in pts:
        if s <= sec:
            best_bid, best_ask = bid, ask
        else:
            break
    return (best_bid, best_ask)


def load_kalshi_ticks(wts_needed):
    """Load Kalshi yes_ask/no_ask ticks as Polymarket book fallback."""
    kb = defaultdict(dict)  # (wts, coin) -> {sec: (yes_ask, no_ask)}
    if not DB.exists(): return kb
    try:
        db = sqlite3.connect(DB)
        for wts in wts_needed:
            for c in ["BTC","ETH","SOL","XRP"]:
                close_dt = datetime.fromtimestamp(wts+900, tz=ET)
                ticker = f"KX{c}15M-{close_dt.strftime('%y%b%d%H%M').upper()}-{close_dt.strftime('%M')}"
                row = db.execute("SELECT id FROM windows WHERE ticker=?", (ticker,)).fetchone()
                if not row: continue
                for sec, ya, na in db.execute(
                    "SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=?", (row[0],)
                ):
                    kb[(wts, c)][sec] = (ya, na)
        db.close()
    except Exception:
        pass
    return kb


def kalshi_at(kb, wts, coin, outcome, sec):
    """Forward-fill Kalshi book. Yes/No are two sides; Up=yes, Down=no.
    bid ≈ 1 - opposite_ask (approximation)."""
    d = kb.get((wts, coin), {})
    if not d: return (None, None)
    found = None
    for s in sorted(d):
        if s <= sec: found = d[s]
        else: break
    if not found: return (None, None)
    ya, na = found
    if outcome == "Up":
        ask = ya if (ya and 0 < ya < 1) else None
        bid = (1.0 - na) if (na and 0 < na < 1) else None
        return (round(bid, 2) if bid else None, ask)
    ask = na if (na and 0 < na < 1) else None
    bid = (1.0 - ya) if (ya and 0 < ya < 1) else None
    return (round(bid, 2) if bid else None, ask)


def main():
    print("Loading…")
    book = load_book()
    btc_trades = load_trades()
    deltas = load_deltas()

    # Group BTC trades by (wts, elapsed_sec)
    trades_at = defaultdict(list)
    trade_wts = set()
    for t in btc_trades:
        wts = t.get("window_start_ts")
        if wts is None: continue
        trade_wts.add(wts)
        sec = int(t.get("elapsed_in_window", 0))
        trades_at[(wts, sec)].append(t)

    # Windows to process: union of Polymarket book + Scallops trade windows
    windows = sorted(set(k[0] for k in book.keys()) | trade_wts)
    # Load Kalshi ticks for all such windows as fallback
    print(f"Loading Kalshi ticks for {len(windows)} windows as fallback…")
    kalshi = load_kalshi_ticks(windows)
    poly_only = len(set(k[0] for k in book.keys()))
    print(f"Windows total: {len(windows)}  (Polymarket book: {poly_only})")
    print(f"BTC 15m trades: {len(btc_trades)}")

    rows = []
    header = [
        "wts", "sec",
        "delta_BTC", "delta_ETH", "delta_SOL", "delta_XRP",
    ]
    for c in COINS:
        header += [f"{c}_yes_bid", f"{c}_yes_ask", f"{c}_no_bid", f"{c}_no_ask"]
    header += [
        "btc_up_shares", "btc_dn_shares",
        "btc_up_avg", "btc_dn_avg",
        "btc_total_cost",
        "btc_if_up", "btc_if_dn",
        # Scale-aware features (cost context)
        "btc_net_exposure",     # |up_shares - dn_shares| — directional imbalance
        "btc_hedge_ratio",      # min(up_cost, dn_cost) / total_cost — how hedged
        "btc_edge_if_up_frac",  # if_up / total_cost — relative payoff Up-side
        "btc_edge_if_dn_frac",  # if_dn / total_cost — relative payoff Dn-side
        "target_profit_up_at_bid",  # if he sold now at Up bid, what's P&L
        "target_profit_dn_at_bid",  # same for Down
        # Targets
        "will_trade",           # binary: did he trade in next BUCKET seconds
        "action", "up_size", "dn_size", "total_size",
        "up_size_pct_of_cost", "dn_size_pct_of_cost",
    ]
    BUCKET = 60  # seconds — binary target: will he trade in next 60s?

    for wts in windows:
        up_cost = dn_cost = 0.0
        up_sh = dn_sh = 0.0
        max_sec = 900

        # Where do we have any book data (Polymarket or Kalshi)?
        btc_up_poly = [s for s, _, _ in book.get((wts, "BTC", "Up"), [])]
        btc_up_kalshi = sorted(kalshi.get((wts, "BTC"), {}).keys())
        sources = []
        if btc_up_poly: sources.append(btc_up_poly[0])
        if btc_up_kalshi: sources.append(btc_up_kalshi[0])
        if not sources: continue
        start_sec = min(sources)
        bucket_start = ((start_sec + BUCKET - 1) // BUCKET) * BUCKET

        for sec in range(bucket_start, max_sec, BUCKET):
            # Always aggregate bucket trades first so state stays consistent
            bucket_trades = []
            up_size = 0.0
            dn_size = 0.0
            for s_offset in range(BUCKET):
                for t in trades_at.get((wts, sec + s_offset), []):
                    bucket_trades.append(t)
                    if t["outcome"] == "Up":
                        up_size += t["notional"]
                    else:
                        dn_size += t["notional"]

            row = {"wts": wts, "sec": sec}
            for c in COINS:
                d = interp(deltas.get((wts, c), []), sec)
                row[f"delta_{c}"] = round(d, 5) if d is not None else None

            complete = True
            for c in COINS:
                u_bid, u_ask = book_at(book, wts, c, "Up", sec)
                d_bid, d_ask = book_at(book, wts, c, "Down", sec)
                if u_ask is None and u_bid is None:
                    u_bid, u_ask = kalshi_at(kalshi, wts, c, "Up", sec)
                if d_ask is None and d_bid is None:
                    d_bid, d_ask = kalshi_at(kalshi, wts, c, "Down", sec)
                row[f"{c}_yes_bid"] = u_bid
                row[f"{c}_yes_ask"] = u_ask
                row[f"{c}_no_bid"]  = d_bid
                row[f"{c}_no_ask"]  = d_ask
                if u_ask is None or d_ask is None:
                    complete = False

            if any(row.get(f"delta_{c}") is None for c in COINS):
                complete = False

            if not complete:
                # State still updates even when we skip emitting
                for t in bucket_trades:
                    if t["outcome"] == "Up":
                        up_cost += t["notional"]; up_sh += t["size"]
                    else:
                        dn_cost += t["notional"]; dn_sh += t["size"]
                continue

            row["btc_up_shares"]  = round(up_sh, 4)
            row["btc_dn_shares"]  = round(dn_sh, 4)
            row["btc_up_avg"]     = round(up_cost / up_sh, 4) if up_sh else 0
            row["btc_dn_avg"]     = round(dn_cost / dn_sh, 4) if dn_sh else 0
            total_cost = up_cost + dn_cost
            row["btc_total_cost"] = round(total_cost, 2)
            if_up = up_sh - total_cost
            if_dn = dn_sh - total_cost
            row["btc_if_up"] = round(if_up, 2)
            row["btc_if_dn"] = round(if_dn, 2)

            # Scale-aware: net imbalance and hedge ratio
            row["btc_net_exposure"] = round(up_sh - dn_sh, 4)
            row["btc_hedge_ratio"]  = round(min(up_cost, dn_cost) / total_cost, 4) if total_cost > 0 else 0
            # Relative P&L (payoff as fraction of cost — matters for sizing)
            row["btc_edge_if_up_frac"] = round(if_up / total_cost, 4) if total_cost > 0 else 0
            row["btc_edge_if_dn_frac"] = round(if_dn / total_cost, 4) if total_cost > 0 else 0
            # Mark-to-market if he sold at current Polymarket bids
            btc_u_bid = row.get("BTC_yes_bid")
            btc_d_bid = row.get("BTC_no_bid")
            tp_up = (up_sh * btc_u_bid + dn_sh * btc_d_bid - total_cost) if (btc_u_bid and btc_d_bid) else 0
            row["target_profit_up_at_bid"] = round((up_sh * (btc_u_bid or 0) - up_cost), 2)
            row["target_profit_dn_at_bid"] = round((dn_sh * (btc_d_bid or 0) - dn_cost), 2)

            # Binary target: did he trade at all in next BUCKET seconds?
            row["will_trade"] = 1 if (up_size + dn_size) > 0 else 0
            # Directional targets (only meaningful when will_trade=1)
            if up_size > 0 and dn_size > 0: row["action"] = "buy_both"
            elif up_size > 0:               row["action"] = "buy_up"
            elif dn_size > 0:               row["action"] = "buy_down"
            else:                           row["action"] = "none"
            row["up_size"] = round(up_size, 2)
            row["dn_size"] = round(dn_size, 2)
            row["total_size"] = round(up_size + dn_size, 2)
            base = max(total_cost, 100.0)
            row["up_size_pct_of_cost"] = round(up_size / base, 4)
            row["dn_size_pct_of_cost"] = round(dn_size / base, 4)
            rows.append(row)

            # Update running state with the bucket's trades
            for t in bucket_trades:
                if t["outcome"] == "Up":
                    up_cost += t["notional"]; up_sh += t["size"]
                else:
                    dn_cost += t["notional"]; dn_sh += t["size"]

    # Write CSV
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Stats
    n = len(rows)
    actions = defaultdict(int)
    for r in rows: actions[r["action"]] += 1
    print(f"\nWrote {n} rows to {OUT_CSV}")
    print(f"Class distribution:")
    for a, c in sorted(actions.items(), key=lambda x: -x[1]):
        print(f"  {a:>12}: {c:>6}  ({c/n*100:.1f}%)")


if __name__ == "__main__":
    main()
