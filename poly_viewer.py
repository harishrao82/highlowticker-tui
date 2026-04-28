#!/usr/bin/env python3
"""
poly_viewer.py — web viewer stitching Polymarket book data with Scallops trades.

Reads:
  ~/.poly_book_snapshots.jsonl — best bid/ask per token per second
  ~/.scallops_live_trades.jsonl — Scallops trades
  ~/.btc_windows.db — Kalshi winners + Coinbase prices for delta

Shows: one row per second per window, 4 coins side-by-side with
  - Polymarket Up/Down bid/ask
  - Coin delta (from trade-interpolation)
  - Any Scallops trades this second

Run: python3 poly_viewer.py
Open: http://localhost:7333
"""
import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

BOOK_LOG = Path.home() / ".poly_book_snapshots.jsonl"
TRADE_LOG = Path.home() / ".scallops_live_trades.jsonl"
SNAPSHOT_LOG = Path.home() / ".kalshi_status_snapshots.jsonl"  # has Coinbase prices
DB = Path.home() / ".btc_windows.db"
ET = timezone(timedelta(hours=-4))

app = FastAPI()


def _ticker_for(coin: str, wts: int) -> str:
    close_dt = datetime.fromtimestamp(wts + 900, tz=ET)
    return (f"KX{coin}15M-"
            + close_dt.strftime("%y%b%d%H%M").upper()
            + "-" + close_dt.strftime("%M"))


def _winners() -> dict:
    w = {}
    if DB.exists():
        try:
            db = sqlite3.connect(DB)
            for ticker, win in db.execute("SELECT ticker, winner FROM windows WHERE winner IS NOT NULL"):
                w[ticker] = win
            db.close()
        except Exception:
            pass
    return w


def _pnl_class(v):
    return "pos" if v is not None and v >= 0 else "neg" if v is not None else "pend"


KNOWN_WALLETS = {
    "scallops": "0xe1d6b51521bd4365769199f392f9818661bd907c",
    "twin": "0x3a847382ad6fff9be1db4e073fd9b869f6884d44",
}


def _fetch_wallet_trades(wallet: str, hours: int) -> list[dict]:
    """Fetch recent trades for a wallet from Polymarket /activity API."""
    import httpx
    trades = []
    for offset in range(0, 500, 100):
        try:
            r = httpx.get("https://data-api.polymarket.com/activity",
                          params={"user": wallet, "limit": 100, "offset": offset},
                          timeout=15)
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            trades.extend(batch)
            if len(batch) < 100:
                break
        except Exception:
            break
    cutoff = time.time() - hours * 3600
    return [t for t in trades if isinstance(t, dict)
            and float(t.get('timestamp', 0)) >= cutoff
            and t.get('side') == 'BUY' and t.get('type') == 'TRADE']


@app.get("/", response_class=HTMLResponse)
def index(hours: int = 6, coin: str = "", book_only: int = 0, trades_only: int = 0,
          wallet: str = ""):
    now = time.time()
    cutoff = now - hours * 3600

    # Resolve wallet shorthand
    wallet_addr = KNOWN_WALLETS.get(wallet.lower(), wallet) if wallet else ""
    wallet_name = wallet or ""

    # Load book snapshots
    # book_by_slot[wts][coin][second][outcome] = (best_bid, bid_sz, best_ask, ask_sz)
    book_by_slot = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    if BOOK_LOG.exists():
        with open(BOOK_LOG) as f:
            for line in f:
                try:
                    e = json.loads(line)
                    wts = e.get("window_start_ts")
                    if not wts or wts < cutoff:
                        continue
                    sec = int(e.get("elapsed", 0))
                    book_by_slot[wts][e["coin"]][sec][e["outcome"]] = (
                        e.get("best_bid"), e.get("bid_size"),
                        e.get("best_ask"), e.get("ask_size"),
                    )
                except Exception:
                    continue

    # Load Scallops trades
    trades_by_slot = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    trades_delta_points = defaultdict(list)  # (wts, coin) -> [(sec, delta)]
    all_trades_count = 0
    if TRADE_LOG.exists():
        with open(TRADE_LOG) as f:
            for line in f:
                try:
                    t = json.loads(line)
                    # Only 15-minute markets
                    if "-15m-" not in t.get("slug", ""):
                        continue
                    wts = t.get("window_start_ts")
                    if not wts or wts < cutoff:
                        continue
                    sec = int(t.get("elapsed_in_window", 0))
                    trades_by_slot[wts][t["coin"]][sec].append(t)
                    all_trades_count += 1
                    if t.get("delta_pct") is not None:
                        trades_delta_points[(wts, t["coin"])].append((sec, t["delta_pct"]))
                except Exception:
                    continue
    for k in trades_delta_points:
        trades_delta_points[k].sort()

    # Load additional wallet trades if requested
    # Twin uses 5-min markets as his primary signal; others use 15-min.
    wallet_slug_filter = '-5m-' if wallet_name.lower() == 'twin' else '-15m-'
    if wallet_addr:
        try:
            extra_trades = _fetch_wallet_trades(wallet_addr, hours)
            for t in extra_trades:
                slug = t.get('slug', '')
                if wallet_slug_filter not in slug:
                    continue
                # Parse slug → coin, window_start_ts
                s = slug.lower()
                coin_map = {'btc': 'BTC', 'eth': 'ETH', 'sol': 'SOL', 'xrp': 'XRP',
                            'solana': 'SOL', 'bitcoin': 'BTC', 'ethereum': 'ETH'}
                c = None
                for prefix, cn in coin_map.items():
                    if s.startswith(prefix):
                        c = cn
                        break
                if not c:
                    continue
                try:
                    wts_raw = int(slug.rsplit('-', 1)[1])
                except Exception:
                    continue
                # For 5m trades, map to the parent 15m window
                wts = (wts_raw // 900) * 900
                trade_ts = int(float(t.get('timestamp', 0)))
                sec = trade_ts - wts
                if sec < 0 or sec > 900:
                    continue
                rec = {
                    "coin": c, "window_start_ts": wts, "slug": slug,
                    "side": t.get("side", ""), "outcome": t.get("outcome", ""),
                    "price": float(t.get("price", 0) or 0),
                    "size": float(t.get("size", 0) or 0),
                    "notional": round(float(t.get("price", 0) or 0) * float(t.get("size", 0) or 0), 2),
                    "elapsed_in_window": sec,
                    "pseudonym": t.get("pseudonym", wallet_name),
                    "_wallet_overlay": True,
                }
                trades_by_slot[wts][c][sec].append(rec)
                all_trades_count += 1
        except Exception:
            pass

    # Also load Coinbase-based delta from Kalshi snapshot log (more granular — every 10s).
    # Map (wts, coin) -> [(elapsed_sec, delta_pct)]
    snap_delta_points = defaultdict(list)
    if SNAPSHOT_LOG.exists():
        try:
            with open(SNAPSHOT_LOG) as f:
                for line in f:
                    try:
                        s = json.loads(line)
                        ts = s.get('ts')
                        coin = s.get('coin')
                        ticker = s.get('ticker', '')
                        dpct = s.get('delta_pct')
                        if not (ts and coin and dpct is not None): continue
                        # Derive wts from ticker — KX{COIN}15M-26APR141515-15 → close time
                        # Simpler: snapshot has 'elapsed' already, and we know wts = ts - elapsed
                        elapsed = s.get('elapsed')
                        if elapsed is None: continue
                        wts = int(ts - elapsed)
                        # Round wts to nearest 15-min boundary (900s)
                        wts = (wts // 900) * 900
                        snap_delta_points[(wts, coin)].append((int(elapsed), dpct))
                    except Exception:
                        continue
        except Exception:
            pass
    for k in snap_delta_points:
        snap_delta_points[k].sort()

    # Also load Kalshi ticks as a fallback book source — gives us yes_ask/no_ask
    # per second for every historical window. Stored as (yes_ask, no_ask) per sec.
    # kalshi_book[(wts, coin)] = dict[sec] = (yes_ask, no_ask)
    kalshi_book = defaultdict(dict)
    if DB.exists():
        try:
            db2 = sqlite3.connect(DB)
            # Cache window_id + wts mapping
            for wts in set(list(book_by_slot.keys()) + list(trades_by_slot.keys())):
                for c in ["BTC","ETH","SOL","XRP"]:
                    close_dt = datetime.fromtimestamp(wts+900, tz=ET)
                    ticker = f"KX{c}15M-{close_dt.strftime('%y%b%d%H%M').upper()}-{close_dt.strftime('%M')}"
                    row = db2.execute("SELECT id FROM windows WHERE ticker=?", (ticker,)).fetchone()
                    if not row: continue
                    wid = row[0]
                    for sec, ya, na in db2.execute(
                        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=?", (wid,)
                    ):
                        kalshi_book[(wts, c)][sec] = (ya, na)
            db2.close()
        except Exception:
            pass

    def kalshi_fallback(wts, coin, outcome, sec):
        """Return (best_bid, best_ask) from Kalshi ticks. Only ask is real; bid = 1-opposite_ask approx."""
        d = kalshi_book.get((wts, coin), {})
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
        else:
            ask = na if (na and 0 < na < 1) else None
            bid = (1.0 - ya) if (ya and 0 < ya < 1) else None
            return (round(bid, 2) if bid else None, ask)

    def _interp_from_pts(pts, sec):
        if not pts: return None
        if sec <= pts[0][0]: return pts[0][1]
        if sec >= pts[-1][0]: return pts[-1][1]
        for i in range(1, len(pts)):
            if pts[i][0] >= sec:
                s0, d0 = pts[i-1]; s1, d1 = pts[i]
                if s1 == s0: return d1
                return d0 + (sec - s0) / (s1 - s0) * (d1 - d0)
        return pts[-1][1]

    def interp_delta(wts, coin, sec):
        # Prefer Kalshi snapshot log (Coinbase prices every ~10s)
        pts = snap_delta_points.get((wts, coin))
        if pts:
            return _interp_from_pts(pts, sec)
        # Fall back to Scallops trade-time deltas
        return _interp_from_pts(trades_delta_points.get((wts, coin)), sec)

    # Also forward-fill book state: once we have a snapshot, it persists until next update
    # Build forward-filled view per (wts, coin, outcome) per second
    def book_at(wts, coin, sec, outcome):
        """Get latest book snapshot at or before this second."""
        coin_data = book_by_slot.get(wts, {}).get(coin, {})
        if not coin_data: return None
        # Walk back from sec to find most recent snapshot with this outcome
        best = None
        for s in sorted(coin_data):
            if s <= sec and outcome in coin_data[s]:
                best = coin_data[s][outcome]
            elif s > sec:
                break
        return best

    # Winners
    winners = _winners()

    # All relevant windows
    all_wts = sorted(set(list(book_by_slot.keys()) + list(trades_by_slot.keys())), reverse=True)
    if coin:
        all_wts = [w for w in all_wts
                   if coin in book_by_slot.get(w, {}) or coin in trades_by_slot.get(w, {})]
    if book_only:
        all_wts = [w for w in all_wts if w in book_by_slot]

    # Build sections
    sections_html = []
    COINS = ["BTC", "ETH", "SOL", "XRP"]

    for wts in all_wts:
        dt = datetime.fromtimestamp(wts, tz=ET)

        # Per coin, compute summary: winner, total trades, total cost, lean, P&L
        coin_summary = {}
        for c in COINS:
            ticker = _ticker_for(c, wts)
            winner_raw = winners.get(ticker)
            winner = "Up" if winner_raw == "yes" else "Down" if winner_raw == "no" else None

            # Scallops trades for this coin in this window
            ctrades = []
            for sec_ts in trades_by_slot.get(wts, {}).get(c, {}).values():
                ctrades.extend(sec_ts)

            up_cost = sum(t["notional"] for t in ctrades if t["outcome"]=="Up" and t["side"]=="BUY")
            dn_cost = sum(t["notional"] for t in ctrades if t["outcome"]=="Down" and t["side"]=="BUY")
            up_sh = sum(t["size"] for t in ctrades if t["outcome"]=="Up" and t["side"]=="BUY")
            dn_sh = sum(t["size"] for t in ctrades if t["outcome"]=="Down" and t["side"]=="BUY")
            cost = up_cost + dn_cost
            pnl = None
            if winner and cost > 0:
                payout = up_sh if winner == "Up" else dn_sh
                pnl = payout - cost
            lean = "Up" if up_cost > dn_cost else "Down" if dn_cost > up_cost else None

            coin_summary[c] = dict(
                winner=winner, n_trades=len(ctrades), cost=cost,
                up_cost=up_cost, dn_cost=dn_cost,
                up_sh=up_sh, dn_sh=dn_sh, lean=lean, pnl=pnl,
            )

        # Generate EVERY second from 0 to last activity (or window end)
        last_sec = 0
        for c in COINS:
            for s in book_by_slot.get(wts, {}).get(c, {}):
                if s > last_sec: last_sec = s
            for s in trades_by_slot.get(wts, {}).get(c, {}):
                if s > last_sec: last_sec = s
        if last_sec == 0:
            all_secs = []
        else:
            # Cap at 900 (window end)
            all_secs = list(range(0, min(last_sec + 1, 901)))

        # Build table rows
        table_rows = []
        # Running totals per coin
        running = {c: {"up_cost":0, "up_sh":0, "dn_cost":0, "dn_sh":0} for c in COINS}

        for sec in all_secs:
            if sec < 0 or sec > 900: continue
            # trades_only: skip rows with no Scallops trades
            if trades_only:
                has_any_trade = any(
                    trades_by_slot.get(wts, {}).get(c, {}).get(sec, [])
                    for c in COINS
                )
                if not has_any_trade:
                    continue
            cells = []
            for c in COINS:
                # Book (forward-filled): each snap is (best_bid, bid_sz, best_ask, ask_sz)
                up_book = book_at(wts, c, sec, "Up")
                dn_book = book_at(wts, c, sec, "Down")
                u_bid = up_book[0] if up_book else None
                u_ask = up_book[2] if up_book else None
                d_bid = dn_book[0] if dn_book else None
                d_ask = dn_book[2] if dn_book else None
                # Fallback to Kalshi ticks if no Polymarket book
                if u_ask is None and u_bid is None:
                    u_bid, u_ask = kalshi_fallback(wts, c, "Up", sec)
                if d_ask is None and d_bid is None:
                    d_bid, d_ask = kalshi_fallback(wts, c, "Down", sec)

                # Delta (interpolated)
                d = interp_delta(wts, c, sec)

                # Trades at this second
                trs = trades_by_slot.get(wts, {}).get(c, {}).get(sec, [])
                for t in trs:
                    if t["side"] != "BUY": continue
                    if t["outcome"] == "Up":
                        running[c]["up_cost"] += t["notional"]
                        running[c]["up_sh"] += t["size"]
                    else:
                        running[c]["dn_cost"] += t["notional"]
                        running[c]["dn_sh"] += t["size"]

                # Format cell content
                parts = []
                # Book line — show Up bid/ask and Down bid/ask
                def _fmt(v):
                    return f"{v:.2f}" if (v is not None and 0 < v < 1) else "—"
                has_book = u_ask or d_ask or u_bid or d_bid
                if has_book:
                    book_parts = [
                        f"<span class='up'>Y {_fmt(u_bid)}/{_fmt(u_ask)}</span>",
                        f"<span class='dn'>N {_fmt(d_bid)}/{_fmt(d_ask)}</span>",
                    ]
                    parts.append(f"<div class='book'>{' '.join(book_parts)}</div>")
                else:
                    parts.append(f"<div class='book dim'>—</div>")

                # Delta line
                if d is not None:
                    d_col = "up" if d > 0 else "dn" if d < 0 else "dim"
                    parts.append(f"<div class='delta'><span class='{d_col}'>Δ{d:+.3f}%</span></div>")

                # Trades this second
                for t in trs:
                    if t["side"] != "BUY": continue
                    oc = "up" if t["outcome"] == "Up" else "dn"
                    oc_s = "U" if t["outcome"] == "Up" else "D"
                    parts.append(
                        f"<div class='trade'>"
                        f"<span class='{oc}'><b>{oc_s}</b></span> "
                        f"@{t['price']:.2f} <span class='money'>${t['notional']:.0f}</span>"
                        f"</div>"
                    )

                # Running total + if-up/if-down
                r = running[c]
                total_cost = r["up_cost"] + r["dn_cost"]
                if total_cost > 0:
                    u_avg = r["up_cost"]/r["up_sh"] if r["up_sh"] else 0
                    d_avg = r["dn_cost"]/r["dn_sh"] if r["dn_sh"] else 0
                    if_up = r["up_sh"] - total_cost
                    if_dn = r["dn_sh"] - total_cost
                    uc = "pos" if if_up >= 0 else "neg"
                    dc = "pos" if if_dn >= 0 else "neg"
                    sums = []
                    if r["up_sh"] > 0:
                        sums.append(f"<span class='up'>U:{r['up_sh']:.0f}sh@{u_avg:.2f}</span>")
                    if r["dn_sh"] > 0:
                        sums.append(f"<span class='dn'>D:{r['dn_sh']:.0f}sh@{d_avg:.2f}</span>")
                    if sums:
                        parts.append(
                            f"<div class='running'>Σ {' / '.join(sums)} "
                            f"<span class='money'>${total_cost:.0f}</span></div>"
                        )
                        parts.append(
                            f"<div class='outcome'>"
                            f"if↑<span class='{uc}'>${if_up:+.1f}</span> "
                            f"if↓<span class='{dc}'>${if_dn:+.1f}</span>"
                            f"</div>"
                        )

                cells.append(f"<td>{''.join(parts)}</td>")
            table_rows.append(f"<tr><td class='sec'>{sec}s</td>{''.join(cells)}</tr>")

        # Summary row
        summary_cells = []
        for c in COINS:
            s = coin_summary[c]
            w_cls = "up" if s["winner"] == "Up" else "dn" if s["winner"] == "Down" else ""
            pnl_cls = _pnl_class(s["pnl"])
            pnl_str = f"${s['pnl']:+,.2f}" if s["pnl"] is not None else "..."
            summary_cells.append(
                f"<td class='summary-cell'>"
                f"<div><b class='{w_cls}'>{s['winner'] or '—'}</b></div>"
                f"<div class='{pnl_cls}'><b>{pnl_str}</b></div>"
                f"<div class='dim'>{s['n_trades']} trades · ${s['cost']:,.0f}</div>"
                f"</td>"
            )
        summary_row = f"<tr class='summary-row'><td class='sec'><b>RESULT</b></td>{''.join(summary_cells)}</tr>"

        total_cost = sum(s["cost"] for s in coin_summary.values())
        total_pnl = sum((s["pnl"] or 0) for s in coin_summary.values() if s["pnl"] is not None)
        slot_pc = _pnl_class(total_pnl) if any(s["pnl"] is not None for s in coin_summary.values()) else "pend"

        coin_badges = ""
        for c in COINS:
            s = coin_summary[c]
            pnl_str = f"${s['pnl']:+,.0f}" if s["pnl"] is not None else "..."
            pc = _pnl_class(s["pnl"])
            coin_badges += f"<span class='coin-badge'>{c}</span><span class='{pc}'>{pnl_str}</span>&nbsp;&nbsp;"

        slot_id = f"slot_{wts}"
        sections_html.append(f"""
        <div class='timeslot' data-slot-id='{slot_id}'>
          <div class='timeslot-header' onclick="toggleSlot('{slot_id}')">
            <span style='font-weight:bold;min-width:130px'>{dt.strftime('%b %d %H:%M ET')}</span>
            {coin_badges}
            <span class='dim' style='margin-left:auto'>${total_cost:,.0f}</span>
            <span class='{slot_pc}' style='font-weight:bold;min-width:70px;text-align:right'>${total_pnl:+,.0f}</span>
          </div>
          <div class='timeslot-body'>
            <table class='matrix'>
              <thead>
                <tr><th class='sec-hdr'>sec</th>
                  {''.join(f'<th>{c}</th>' for c in COINS)}
                </tr>
              </thead>
              <tbody>
                {summary_row}
                {''.join(table_rows)}
              </tbody>
            </table>
          </div>
        </div>""")

    # KPIs
    total_windows = len(all_wts)
    total_trades = all_trades_count
    total_updates = sum(
        sum(len(by_sec) for by_sec in c_data.values())
        for c_data in book_by_slot.values()
    )

    coin_tabs = [f"<a href='/?hours={hours}' class='{'active' if not coin else ''}'>All</a>"]
    for c in COINS:
        coin_tabs.append(
            f"<a href='/?hours={hours}&coin={c}' class='{'active' if coin==c else ''}'>{c}</a>"
        )

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset='utf-8'><title>Poly Viewer</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a0a0f; color: #e0e0e0; font-family: 'SF Mono','Fira Code',monospace; font-size: 12px; padding: 14px; }}
  h1 {{ color: #fbbf24; font-size: 18px; margin-bottom: 4px; }}
  .meta {{ color: #555; font-size: 11px; margin-bottom: 12px; }}
  .up {{ color: #4ade80; }} .dn {{ color: #f87171; }}
  .pos {{ color: #4ade80; }} .neg {{ color: #f87171; }}
  .dim {{ color: #555; }} .pend {{ color: #888; }}
  .money {{ color: #fbbf24; }}

  .tabs {{ display: flex; gap: 8px; margin: 12px 0; flex-wrap: wrap; }}
  .tabs a {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px;
             padding: 6px 14px; color: #e0e0e0; text-decoration: none; font-size: 12px; }}
  .tabs a.active {{ background: #1e1e3a; border-color: #60a5fa; color: #60a5fa; }}

  .kpis {{ display: flex; gap: 16px; margin: 12px 0; flex-wrap: wrap; }}
  .kpi {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px; padding: 10px 18px; }}
  .kpi-val {{ font-size: 20px; font-weight: bold; }}
  .kpi-label {{ color: #555; font-size: 11px; margin-top: 2px; }}

  .timeslot {{ margin-bottom: 4px; }}
  .timeslot-header {{ display: flex; align-items: center; gap: 8px; padding: 8px 12px;
                      background: #111128; border: 1px solid #1e1e3a; border-radius: 6px;
                      cursor: pointer; flex-wrap: wrap; }}
  .timeslot-header:hover {{ background: #1a1a2e; }}
  .timeslot-body {{ padding: 8px 0 12px 0; display: none; }}
  .coin-badge {{ background: #1e1e3a; color: #fbbf24; font-weight: bold;
                 padding: 2px 8px; border-radius: 4px; }}

  .matrix {{ width: 100%; border-collapse: collapse; font-size: 11px; table-layout: fixed; }}
  .matrix th {{ background: #1a1a2e; color: #60a5fa; padding: 6px 8px; text-align: left;
                font-size: 10px; text-transform: uppercase; border-bottom: 1px solid #2a2a4a; }}
  .matrix th.sec-hdr {{ width: 55px; }}
  .matrix td {{ padding: 4px 8px; border-bottom: 1px solid #111; vertical-align: top;
                width: 23%; }}
  .matrix td.sec {{ color: #666; width: 55px; font-family: monospace; }}
  .matrix .book {{ font-size: 11px; color: #aaa; }}
  .matrix .delta {{ font-size: 10px; }}
  .matrix .trade {{ font-size: 11px; font-weight: bold; margin-top: 2px; }}
  .matrix .running {{ font-size: 10px; margin-top: 3px; padding-top: 3px;
                      border-top: 1px dashed #222; color: #888; }}
  .matrix .outcome {{ font-size: 10px; color: #888; }}
  .matrix tr:hover td {{ background: #12122a; }}
  .matrix .summary-row td {{ background: #0e0e1c; padding: 8px 10px; border-bottom: 2px solid #2a2a4a; }}
  .matrix .summary-cell {{ font-size: 12px; line-height: 1.5; }}
</style></head>
<body>
<h1>Polymarket Book + Scallops Trades Viewer
  <button onclick='location.reload()' style='float:right;background:#1e1e3a;border:1px solid #60a5fa;color:#60a5fa;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:12px'>↻ refresh</button>
</h1>
<div class='meta'>{datetime.now(tz=ET).strftime('%H:%M:%S ET')} · manual refresh</div>

<div>
  <a href='/?hours=2'>2h</a> ·
  <a href='/?hours=6'>6h</a> ·
  <a href='/?hours=12'>12h</a> ·
  <a href='/?hours=24'>24h</a>
  &nbsp;&nbsp;&nbsp;
  <a href='/?hours={hours}&book_only={1 if not book_only else 0}' style='{'color:#4ade80' if book_only else ''}'>
    {'★ book-only' if book_only else 'book-only'}
  </a>
  &nbsp;·&nbsp;
  <a href='/?hours={hours}&trades_only={1 if not trades_only else 0}' style='{'color:#f59e0b' if trades_only else ''}'>
    {'★ trades-only' if trades_only else 'trades-only'}
  </a>
  &nbsp;&nbsp;&nbsp;
  <a href='/?hours={hours}&trades_only=1&wallet=twin' style='{'color:#e879f9' if wallet_name == 'twin' else ''}'>Twin</a>
  ·
  <a href='/?hours={hours}&trades_only=1&wallet=' style='{'color:#e879f9' if not wallet_name else ''}'>Scallops only</a>
</div>

<div class='tabs'>{"".join(coin_tabs)}</div>

<div class='kpis'>
  <div class='kpi'><div class='kpi-val'>{total_windows}</div><div class='kpi-label'>Windows</div></div>
  <div class='kpi'><div class='kpi-val'>{total_trades:,}</div><div class='kpi-label'>Scallops trades</div></div>
  <div class='kpi'><div class='kpi-val'>{total_updates:,}</div><div class='kpi-label'>Book updates</div></div>
</div>

{"".join(sections_html)}

<script>
function expandedSlots() {{
  try {{ return new Set(JSON.parse(localStorage.getItem('poly_expanded') || '[]')); }}
  catch (e) {{ return new Set(); }}
}}
function saveExpanded(set) {{
  localStorage.setItem('poly_expanded', JSON.stringify([...set]));
}}
function toggleSlot(id) {{
  var el = document.querySelector("[data-slot-id='" + id + "'] .timeslot-body");
  if (!el) return;
  var now = getComputedStyle(el).display === 'block' ? 'none' : 'block';
  el.style.display = now;
  var s = expandedSlots();
  if (now === 'block') s.add(id); else s.delete(id);
  saveExpanded(s);
}}
(function() {{
  var s = expandedSlots();
  s.forEach(function(id) {{
    var el = document.querySelector("[data-slot-id='" + id + "'] .timeslot-body");
    if (el) el.style.display = 'block';
  }});
  var ls = parseInt(sessionStorage.getItem('poly_scroll') || '0', 10);
  window.scrollTo(0, ls);
  window.addEventListener('scroll', function() {{
    sessionStorage.setItem('poly_scroll', String(window.scrollY));
  }});
}})();
</script>
</body></html>"""
    return HTMLResponse(html)


if __name__ == "__main__":
    print("Poly Viewer → http://localhost:7333")
    uvicorn.run(app, host="0.0.0.0", port=7333, log_level="warning")
