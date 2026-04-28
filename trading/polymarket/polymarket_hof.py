#!/usr/bin/env python3
"""
Polymarket Hall of Fame Tracker.

Watches ~/.polymarket_hof.json and shows ALL trades from tracked traders.
No spend/odds filter — every bet they make appears.
Picks up new additions to the HoF file automatically every 30s.
Run: python3 polymarket_hof.py
"""
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path

import httpx
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Static, Footer
from rich.text import Text
from rich.style import Style

HOF_FILE   = Path.home() / ".polymarket_hof.json"
TRADES_API = "https://data-api.polymarket.com/trades"
GAMMA_API  = "https://gamma-api.polymarket.com/markets"
POLL_SECS  = 5    # how often to check each HoF wallet for new trades
HOF_CHECK  = 5    # how often to re-read the HoF file for new additions

# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_name(name: str, wallet: str) -> str:
    if not name or name.lower().startswith(wallet.lower()[:6].lower()):
        return "anon"
    if len(name) > 30 and "-" in name and name.replace("-", "").replace("0x", "").isalnum():
        return "anon"
    return name


def _make_hit(t: dict, hof_name: str = "") -> dict:
    price  = float(t.get("price") or 0)
    size   = float(t.get("size")  or 0)
    wallet = t.get("proxyWallet", "")
    tx     = t.get("transactionHash", "")
    name   = _clean_name(t.get("name") or t.get("pseudonym") or "", wallet)
    if name == "anon" and hof_name:
        name = hof_name
    return {
        "title":      t.get("title", "Unknown market"),
        "outcome":    t.get("outcome", "?"),
        "side":       t.get("side", "?"),
        "price":      price,
        "size":       size,
        "spent":      price * size,
        "payout":     size,
        "ts":         float(t.get("timestamp") or 0),
        "tx":         tx,
        "slug":       t.get("slug", ""),
        "wallet":     wallet,
        "name":       name,
        "resolution": "",
    }


def _load_hof() -> dict:
    try:
        return json.loads(HOF_FILE.read_text()) if HOF_FILE.exists() else {}
    except Exception:
        return {}


def get_market_outcome(slug: str, outcome: str) -> str:
    if not slug:
        return ""
    try:
        r = httpx.get(GAMMA_API, params={"slug": slug}, timeout=10)
        if r.status_code != 200:
            return ""
        data = r.json()
        if isinstance(data, list):
            data = data[0] if data else {}
    except Exception:
        return ""
    if not data.get("closed"):
        return ""
    raw_prices   = data.get("outcomePrices")
    raw_outcomes = data.get("outcomes")
    try:
        prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else (raw_prices   or [])
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else (raw_outcomes or [])
    except (json.JSONDecodeError, TypeError):
        return ""
    idx = next((i for i, o in enumerate(outcomes)
                if str(o).strip().lower() == outcome.strip().lower()), None)
    if idx is None or idx >= len(prices):
        return ""
    try:
        p = float(prices[idx])
    except (TypeError, ValueError):
        return ""
    if p >= 0.99:   return "WIN"
    elif p <= 0.01: return "LOSS"
    return "VOID"


def fetch_wallet_trades(wallet: str, hof_name: str, limit: int = 100) -> list[dict]:
    """Fetch recent trades for a wallet — all BUY trades, no filter."""
    try:
        r = httpx.get(TRADES_API, params={"user": wallet, "limit": limit}, timeout=12)
        if r.status_code != 200:
            return []
        trades = r.json()
    except Exception:
        return []
    hits = []
    for t in trades:
        if t.get("side") != "BUY":
            continue
        price = float(t.get("price") or 0)
        size  = float(t.get("size")  or 0)
        if price <= 0 or size <= 0:
            continue
        hits.append(_make_hit(t, hof_name))
    return hits


# ── TUI ────────────────────────────────────────────────────────────────────────

WIN_STYLE  = Style(bgcolor="rgb(5,46,22)")
LOSS_STYLE = Style(bgcolor="rgb(69,10,10)")
NEW_STYLE  = Style(bgcolor="rgb(30,58,138)")

# Each HoF trader gets a distinct row tint — cycles through these
TRADER_COLORS = [
    Style(bgcolor="rgb(78,63,0)"),    # gold
    Style(bgcolor="rgb(30,50,80)"),   # steel blue
    Style(bgcolor="rgb(60,30,70)"),   # purple
    Style(bgcolor="rgb(20,60,40)"),   # forest
    Style(bgcolor="rgb(70,40,20)"),   # bronze
    Style(bgcolor="rgb(50,20,30)"),   # crimson
]


def _status_text(res: str) -> Text:
    if res == "WIN":  return Text("✓ WIN",  style="bold rgb(74,222,128)")
    if res == "LOSS": return Text("✗ LOSS", style="bold rgb(220,38,38)")
    if res == "VOID": return Text("~ VOID", style="dim")
    return Text("open", style="rgb(234,179,8)")


def _row_style(res: str, is_new: bool, trader_style: Style) -> Style:
    if res == "WIN":  return WIN_STYLE
    if res == "LOSS": return LOSS_STYLE
    if is_new:        return NEW_STYLE
    return trader_style


class HoFTrackerApp(App):
    CSS = """
    Screen { layout: vertical; background: #0a0a0f; }
    #status-bar { height: 2; padding: 0 1; background: #111128; border-bottom: solid rgb(234,179,8); }
    #main-area { height: 1fr; layout: horizontal; }
    #table-box { width: 65%; height: 1fr; border: solid rgb(234,179,8); }
    #detail-box { width: 35%; height: 1fr; border: solid rgb(234,179,8); padding: 0 0; }
    DataTable { height: 1fr; }
    #detail-title { height: 1; background: #111128; padding: 0 1;
                    border-bottom: dashed rgb(234,179,8); color: rgb(234,179,8); }
    #detail-body { height: 1fr; padding: 1 1; overflow-y: auto; overflow-x: hidden; }
    #summary-title { height: 1; background: #111128; padding: 0 1;
                     border-top: solid rgb(234,179,8); border-bottom: dashed rgb(234,179,8);
                     color: rgb(234,179,8); }
    #summary-body { height: auto; min-height: 4; max-height: 12; padding: 0 1;
                    overflow-y: auto; overflow-x: hidden; }
    Footer { background: #111128; }
    """
    BINDINGS = [
        Binding("q", "quit",           "Quit"),
        Binding("r", "force_poll",     "Poll now"),
        Binding("c", "clear_resolved", "Clear resolved"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._hof:          dict[str, dict] = {}
        self._hof_mtime:    float           = 0.0
        self._trader_color: dict[str, Style] = {}  # wallet -> style
        self._seen:         set[str]         = set()
        self._pending:      list[dict]       = []
        self._rows:         dict[str, dict]  = {}
        self._last_poll     = 0.0
        self._status_msg    = "Loading HoF..."
        self._selected_tx:  str | None       = None

    def compose(self) -> ComposeResult:
        yield Static("", id="status-bar")
        with Horizontal(id="main-area"):
            with Vertical(id="table-box"):
                yield DataTable(id="trades-table", cursor_type="row", zebra_stripes=False)
            with Vertical(id="detail-box"):
                yield Static("★ HALL OF FAME TRACKER", id="detail-title")
                yield Static("← select a row", id="detail-body")
                yield Static("$ TRADER TOTALS", id="summary-title")
                yield Static("", id="summary-body")
        yield Footer()

    async def on_mount(self) -> None:
        t = self.query_one("#trades-table", DataTable)
        t.add_column("Trader",   key="trader",    width=14)
        t.add_column("Time",     key="time",      width=6)
        t.add_column("Cap'd",    key="captured",  width=6)
        t.add_column("Market",   key="market",    width=28)
        t.add_column("Outcome",  key="outcome",   width=10)
        t.add_column("Odds",     key="odds",      width=6)
        t.add_column("Spent",    key="spent",     width=9)
        t.add_column("Wins",     key="wins",      width=9)
        t.add_column("Ret%",     key="ret",       width=5)
        t.add_column("Status",   key="status",    width=7)
        self.set_interval(1,         self._tick_status)
        self.set_interval(POLL_SECS, self._poll_all)
        self.set_interval(HOF_CHECK, self._reload_hof)
        self.set_interval(60,        self._check_pending)
        asyncio.create_task(self._startup())

    async def _startup(self) -> None:
        await self._reload_hof()
        if not self._hof:
            self._status_msg = "No traders in HoF yet. Add them from polymarket_scan.py"
            return
        self._status_msg = f"Loading trades for {len(self._hof)} trader(s)..."
        # Collect all trades across all wallets, sort by time, then add in order
        all_trades: list[dict] = []
        results = await asyncio.gather(*[
            asyncio.to_thread(fetch_wallet_trades, wallet, entry.get("name", ""), 50)
            for wallet, entry in self._hof.items()
        ], return_exceptions=True)
        for trades in results:
            if isinstance(trades, Exception) or not trades:
                continue
            all_trades.extend(trades)
        all_trades.sort(key=lambda h: h.get("ts", 0))
        for h in all_trades:
            if h["tx"] in self._seen:
                continue
            self._seen.add(h["tx"])
            self._add_row(h, is_new=False)
            if h.get("slug"):
                self._pending.append(h)
        self._last_poll = time.time()

    async def _reload_hof(self) -> None:
        """Re-read HoF file and pick up any new traders added by the scanner."""
        try:
            mtime = HOF_FILE.stat().st_mtime if HOF_FILE.exists() else 0
        except Exception:
            return
        if mtime <= self._hof_mtime:
            return  # file unchanged
        self._hof_mtime = mtime
        new_hof = await asyncio.to_thread(_load_hof)
        # Assign colors to new traders
        existing = set(self._hof.keys())
        for i, wallet in enumerate(new_hof.keys()):
            if wallet not in self._trader_color:
                self._trader_color[wallet] = TRADER_COLORS[len(self._trader_color) % len(TRADER_COLORS)]
        added = [w for w in new_hof if w not in existing]
        self._hof = new_hof
        if added:
            self._status_msg = f"HoF updated — {len(added)} new trader(s) added"
            # Fetch all new traders concurrently, sort by time, then add rows
            results = await asyncio.gather(*[
                asyncio.to_thread(fetch_wallet_trades, w, self._hof[w].get("name", ""), 100)
                for w in added
            ], return_exceptions=True)
            all_trades: list[dict] = []
            for trades in results:
                if isinstance(trades, Exception) or not trades:
                    continue
                all_trades.extend(trades)
            all_trades.sort(key=lambda h: h.get("ts", 0))
            for h in all_trades:
                if h["tx"] in self._seen:
                    continue
                self._seen.add(h["tx"])
                self._add_row(h, is_new=False)
                if h.get("slug"):
                    self._pending.append(h)
            asyncio.create_task(self._resolve_batch(self._pending))

    async def _poll_all(self) -> None:
        """Poll each HoF wallet for new trades."""
        if not self._hof:
            return
        results = await asyncio.gather(*[
            asyncio.to_thread(fetch_wallet_trades, wallet, entry.get("name", ""), 50)
            for wallet, entry in self._hof.items()
        ], return_exceptions=True)
        new_hits: list[dict] = []
        for trades in results:
            if isinstance(trades, Exception) or not trades:
                continue
            for h in trades:
                if h["tx"] in self._seen:
                    continue
                self._seen.add(h["tx"])
                new_hits.append(h)
        new_hits.sort(key=lambda h: h.get("ts", 0))
        for h in new_hits:
            self._add_row(h, is_new=True)
            if h.get("slug"):
                self._pending.append(h)
        self._last_poll = time.time()

    async def _resolve_batch(self, bets: list[dict]) -> None:
        seen_keys: dict[str, str] = {}
        async def _one(bet: dict) -> None:
            key = f"{bet['slug']}|{bet['outcome']}"
            if key not in seen_keys:
                res = await asyncio.to_thread(get_market_outcome, bet["slug"], bet["outcome"])
                seen_keys[key] = res
            else:
                res = seen_keys[key]
            if res in ("WIN", "LOSS", "VOID"):
                bet["resolution"] = res
                self._update_row_style(bet)
                self._pending = [p for p in self._pending if p["tx"] != bet["tx"]]
        await asyncio.gather(*[_one(b) for b in bets], return_exceptions=True)

    async def _check_pending(self) -> None:
        if not self._pending:
            return
        async def _one(bet: dict) -> tuple[dict, str]:
            return bet, await asyncio.to_thread(get_market_outcome, bet["slug"], bet["outcome"])
        results = await asyncio.gather(*[_one(b) for b in list(self._pending)],
                                       return_exceptions=True)
        still_open = []
        for result in results:
            if isinstance(result, Exception):
                continue
            bet, res = result
            if res in ("WIN", "LOSS", "VOID"):
                bet["resolution"] = res
                self._update_row_style(bet)
            else:
                still_open.append(bet)
        self._pending = still_open

    def _trader_style(self, wallet: str) -> Style:
        return self._trader_color.get(wallet, TRADER_COLORS[0])

    def _add_row(self, h: dict, is_new: bool) -> None:
        if h["tx"] in self._rows:
            return
        h.setdefault("captured", datetime.now().strftime("%H:%M"))
        table   = self.query_one("#trades-table", DataTable)
        implied = (1 / h["price"] - 1) * 100 if h["price"] > 0 else 0
        trade_t = datetime.fromtimestamp(h["ts"]).strftime("%H:%M") if h["ts"] else "?"
        market  = (h["title"][:26] + "…") if len(h["title"]) > 27 else h["title"]
        ts      = self._trader_style(h.get("wallet", ""))
        rs      = _row_style(h.get("resolution", ""), is_new, ts)
        trader  = h["name"][:14]
        table.add_row(
            Text(trader,                style=Style.combine([rs, Style(bold=True)])),
            Text(trade_t,               style=rs),
            Text(h["captured"],         style=Style.combine([rs, Style(color="rgb(120,120,160)")])),
            Text(market,                style=rs),
            Text(h["outcome"][:10],     style=rs),
            Text(f"{h['price']:.3f}",   style=Style.combine([rs, Style(color="cyan")])),
            Text(f"${h['spent']:,.0f}", style=Style.combine([rs, Style(bold=True)])),
            Text(f"${h['payout']:,.0f}",style=rs),
            Text(f"{implied:.0f}%",     style=rs),
            _status_text(h.get("resolution", "")),
            key=h["tx"],
        )
        self._rows[h["tx"]] = h
        self._refresh_summary()
        if is_new:
            table.scroll_end(animate=False)
            table.move_cursor(row=table.row_count - 1)

    def _update_row_style(self, h: dict) -> None:
        table   = self.query_one("#trades-table", DataTable)
        ts      = self._trader_style(h.get("wallet", ""))
        rs      = _row_style(h.get("resolution", ""), False, ts)
        implied = (1 / h["price"] - 1) * 100 if h["price"] > 0 else 0
        trade_t = datetime.fromtimestamp(h["ts"]).strftime("%H:%M") if h["ts"] else "?"
        market  = (h["title"][:26] + "…") if len(h["title"]) > 27 else h["title"]
        try:
            table.update_cell(h["tx"], "trader",   Text(h["name"][:14],          style=Style.combine([rs, Style(bold=True)])))
            table.update_cell(h["tx"], "time",     Text(trade_t,                  style=rs))
            table.update_cell(h["tx"], "captured", Text(h.get("captured", "?"),   style=Style.combine([rs, Style(color="rgb(120,120,160)")])))
            table.update_cell(h["tx"], "market",   Text(market,                   style=rs))
            table.update_cell(h["tx"], "outcome", Text(h["outcome"][:10],        style=rs))
            table.update_cell(h["tx"], "odds",    Text(f"{h['price']:.3f}",      style=Style.combine([rs, Style(color="cyan")])))
            table.update_cell(h["tx"], "spent",   Text(f"${h['spent']:,.0f}",    style=Style.combine([rs, Style(bold=True)])))
            table.update_cell(h["tx"], "wins",    Text(f"${h['payout']:,.0f}",   style=rs))
            table.update_cell(h["tx"], "ret",     Text(f"{implied:.0f}%",        style=rs))
            table.update_cell(h["tx"], "status",  _status_text(h["resolution"]))
            self._refresh_summary()
        except Exception:
            pass

    def _tick_status(self) -> None:
        sb    = self.query_one("#status-bar", Static)
        now   = datetime.now().strftime("%H:%M:%S")
        lag   = int(time.time() - self._last_poll) if self._last_poll else 0
        pulse = "·" * (int(time.time()) % 4 + 1)
        extra = f"  [dim]{self._status_msg}[/dim]" if self._status_msg else ""
        traders = "  ".join(
            f"[rgb(234,179,8)]{e.get('name', w[:8])}[/rgb(234,179,8)]"
            for w, e in self._hof.items()
        )
        sb.update(
            f"[bold rgb(234,179,8)]★ HoF Tracker[/bold rgb(234,179,8)]  │  "
            f"{traders or '[dim]no traders yet[/dim]'}  │  "
            f"[cyan]{pulse}[/cyan] poll [cyan]{lag}s[/cyan] ago  │  "
            f"{len(self._rows)} trades  │  "
            f"[yellow]{len(self._pending)} pending[/yellow]  │  {now}{extra}"
        )

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        tx = str(event.row_key.value)
        self._selected_tx = tx
        h  = self._rows.get(tx)
        if not h:
            return
        self._render_detail(h)

    def _refresh_summary(self) -> None:
        """Recompute per-trader totals and update the summary panel."""
        # Aggregate per wallet
        stats: dict[str, dict] = {}
        for h in self._rows.values():
            w = h.get("wallet", "")
            if w not in stats:
                stats[w] = {
                    "name": h.get("name", "anon"),
                    "spent": 0.0, "won": 0.0,
                    "open_spent": 0.0, "open_payout": 0.0,
                    "wins": 0, "losses": 0, "open": 0,
                }
            res    = h.get("resolution", "")
            spent  = h.get("spent", 0.0)
            payout = h.get("payout", 0.0)
            if res == "WIN":
                stats[w]["spent"]  += spent
                stats[w]["won"]    += payout
                stats[w]["wins"]   += 1
            elif res == "LOSS":
                stats[w]["spent"]  += spent
                stats[w]["losses"] += 1
            else:  # open / pending
                stats[w]["open_spent"]  += spent
                stats[w]["open_payout"] += payout
                stats[w]["open"]        += 1

        content = Text()
        for _, s in stats.items():
            net = s["won"] - s["spent"]  # resolved net only

            content.append(f" {s['name'][:16]:<16}", Style(bold=True, color="rgb(234,179,8)"))
            content.append("\n")
            # Resolved row
            content.append("  Resolved  : ", "dim")
            content.append(f"spent ${s['spent']:>7,.0f}", "white")
            content.append("  won ")
            content.append(f"${s['won']:>7,.0f}", "rgb(74,222,128)" if s["won"] > 0 else "dim")
            content.append("\n")
            # Net
            net_style = "rgb(74,222,128)" if net >= 0 else "rgb(220,38,38)"
            content.append("  Net P&L   : ", "dim")
            sign = "+" if net >= 0 else ""
            content.append(f"{sign}${net:,.0f}", f"bold {net_style}")
            content.append(f"  ({s['wins']}W / {s['losses']}L)", "dim")
            content.append("\n")
            # Open bets
            content.append("  Open bets : ", "dim")
            content.append(f"{s['open']} bet(s)", "cyan")
            content.append("  at risk ")
            content.append(f"${s['open_spent']:,.0f}", "bold cyan")
            content.append("  → pot ")
            content.append(f"${s['open_payout']:,.0f}", "yellow")
            content.append("\n\n")

        self.query_one("#summary-body", Static).update(content)

    def _render_detail(self, h: dict) -> None:
        body        = self.query_one("#detail-body", Static)
        implied     = (1 / h["price"] - 1) * 100 if h["price"] > 0 else 0
        res         = h.get("resolution", "")
        trade_t     = datetime.fromtimestamp(h["ts"]).strftime("%H:%M:%S") if h["ts"] else "?"
        market_url  = f"https://polymarket.com/event/{h['slug']}" if h.get("slug") else ""
        profile_url = f"https://polymarket.com/profile/{h['wallet']}" if h.get("wallet") else ""
        hof_entry   = self._hof.get(h.get("wallet", ""), {})
        C = "cyan"; BC = "bold cyan"

        def _kv(k, v): t = Text(); t.append(k, C); t.append(f" {v}"); return t
        def _url(k, u): t = Text(); t.append(k, C); t.append(" "); t.append(u, Style(color="bright_black", link=u)); return t
        def _sep(label): t = Text(); t.append(f"━━ {label} ", BC); t.append("━" * max(0, 38 - len(label)), BC); return t

        content = Text()
        def _add(t):
            if isinstance(t, str): content.append(t + "\n")
            else: content.append_text(t); content.append("\n")

        res_map = {"WIN": ("✓ WIN", "bold rgb(74,222,128)"),
                   "LOSS": ("✗ LOSS", "bold rgb(220,38,38)"),
                   "VOID": ("~ VOID", "dim")}
        res_txt, res_sty = res_map.get(res, ("open", "rgb(234,179,8)"))

        _add(_sep("★ HALL OF FAME TRADER"))
        nl = Text(); nl.append("Name   : ", C); nl.append(h["name"], "bold rgb(234,179,8)")
        if hof_entry.get("added"):
            nl.append(f"  (tracked since {hof_entry['added']})", "dim")
        _add(nl)
        if profile_url:
            _add(_url("Profile:", profile_url))

        _add("")
        _add(_sep("BET"))
        t = Text(); t.append("Time   : ", C); t.append(f"{trade_t}   "); t.append(res_txt, res_sty); _add(t)
        _add(_kv("Outcome:", f"{h['outcome']}  @ {h['price']:.3f} odds  ({implied:.0f}% return)"))
        t = Text(); t.append("Spent  : ", C); t.append(f"${h['spent']:,.0f}", "bold")
        t.append("  →  wins "); t.append(f"${h['payout']:,.0f}", "bold"); t.append(" if hits"); _add(t)
        _add("")
        _add(_kv("Market :", h["title"]))
        if market_url:
            _add(_url("Link   :", market_url))

        body.update(content)

    async def action_force_poll(self) -> None:
        await self._poll_all()

    def action_clear_resolved(self) -> None:
        table = self.query_one("#trades-table", DataTable)
        for tx in [tx for tx, h in self._rows.items()
                   if h.get("resolution") in ("WIN", "LOSS", "VOID")]:
            try: table.remove_row(tx)
            except Exception: pass
            self._rows.pop(tx, None)


if __name__ == "__main__":
    HoFTrackerApp().run()
