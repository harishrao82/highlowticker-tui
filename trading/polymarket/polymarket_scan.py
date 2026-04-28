#!/usr/bin/env python3
"""
Polymarket Whale Scanner — filter-based live feed.

Shows trades matching: spent >= MIN_SPEND and odds <= MAX_PRICE.
Press h on a selected row to add/remove the trader from Hall of Fame.
Run: python3 polymarket_scan.py
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
MIN_SPEND  = 10000     # minimum dollars spent on a single trade
MAX_PRICE  = 0.5    # maximum odds
POLL_SECS  = 30      # live poll interval
PAGE_SIZE  = 1000    # history page size

# ── Shared helpers ─────────────────────────────────────────────────────────────

def _clean_name(name: str, wallet: str) -> str:
    if not name or name.lower().startswith(wallet.lower()[:6].lower()):
        return "anon"
    if len(name) > 30 and "-" in name and name.replace("-", "").replace("0x", "").isalnum():
        return "anon"
    return name


def _make_hit(t: dict) -> dict:
    price  = float(t.get("price") or 0)
    size   = float(t.get("size")  or 0)
    wallet = t.get("proxyWallet", "")
    tx     = t.get("transactionHash", "")
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
        "name":       _clean_name(t.get("name") or t.get("pseudonym") or "", wallet),
        "resolution": "",
    }


def _load_hof() -> dict:
    try:
        return json.loads(HOF_FILE.read_text()) if HOF_FILE.exists() else {}
    except Exception:
        return {}


def _save_hof(hof: dict) -> None:
    try:
        HOF_FILE.write_text(json.dumps(hof, indent=2))
    except Exception:
        pass


def fetch_trades(limit: int = PAGE_SIZE) -> list[dict]:
    r = httpx.get(TRADES_API, params={"limit": limit}, timeout=15)
    r.raise_for_status()
    return r.json()


def _fetch_wallet_recent(wallet: str, limit: int = 50) -> list[dict]:
    """Fetch recent trades for a specific wallet."""
    try:
        r = httpx.get(TRADES_API, params={"user": wallet, "limit": limit}, timeout=12)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def filter_trades(trades: list[dict]) -> list[dict]:
    hits = []
    for t in trades:
        if t.get("side") != "BUY":
            continue
        price = float(t.get("price") or 0)
        size  = float(t.get("size")  or 0)
        if price <= 0 or size <= 0 or price > MAX_PRICE:
            continue
        if price * size < MIN_SPEND:
            continue
        hits.append(_make_hit(t))
    return hits


def fetch_today_history() -> list[dict]:
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    hits, offset, seen_tx = [], 0, set()
    while True:
        try:
            r = httpx.get(TRADES_API, params={"limit": PAGE_SIZE, "offset": offset}, timeout=15)
            if r.status_code == 400:
                break
            r.raise_for_status()
            trades = r.json()
        except Exception:
            break
        if not trades:
            break
        oldest_ts = None
        for t in trades:
            ts = float(t.get("timestamp") or 0)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
            if ts < today_start:
                continue
            tx = t.get("transactionHash", "")
            if tx in seen_tx:
                continue
            seen_tx.add(tx)
            if t.get("side") != "BUY":
                continue
            price = float(t.get("price") or 0)
            size  = float(t.get("size")  or 0)
            if price <= 0 or size <= 0 or price > MAX_PRICE:
                continue
            if price * size < MIN_SPEND:
                continue
            hits.append(_make_hit(t))
        if oldest_ts is not None and oldest_ts < today_start:
            break
        offset += PAGE_SIZE
    return sorted(hits, key=lambda h: h["ts"])


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
    if p >= 0.99:
        return "WIN"
    elif p <= 0.01:
        return "LOSS"
    return "VOID"


def _fetch_user_trades(wallet: str) -> list[dict]:
    trades, offset = [], 0
    while True:
        try:
            r = httpx.get(TRADES_API,
                          params={"user": wallet, "limit": 1000, "offset": offset}, timeout=12)
            if r.status_code == 400:
                break
            page = r.json() if r.status_code == 200 else []
        except Exception:
            break
        if not page:
            break
        trades.extend(page)
        if len(page) < 1000:
            break
        offset += 1000
    return trades


def _val(t: dict) -> float:
    try:
        return float(t.get("price") or 0) * float(t.get("size") or 0)
    except (TypeError, ValueError):
        return 0.0


def get_user_stats(wallet: str) -> dict:
    trades     = _fetch_user_trades(wallet)
    buy_trades = [t for t in trades if t.get("side") == "BUY"]
    total      = len(trades)
    volume     = sum(_val(t) for t in trades)
    avg_bet    = volume / total if total else 0
    big_bets   = sum(1 for t in buy_trades if _val(t) >= MIN_SPEND)
    low_odds   = sum(1 for t in buy_trades if float(t.get("price") or 1) <= MAX_PRICE)
    whale_trades = [t for t in buy_trades
                    if _val(t) >= MIN_SPEND and float(t.get("price") or 1) <= MAX_PRICE]
    whale_spent = sum(_val(t) for t in whale_trades)
    seen_slugs: dict[str, str] = {}
    whale_wins = whale_losses = whale_open = 0
    whale_won_amt = whale_lost_amt = whale_open_amt = 0.0
    for t in whale_trades:
        slug  = t.get("slug", "")
        out   = t.get("outcome", "")
        key   = f"{slug}|{out}"
        if key not in seen_slugs:
            seen_slugs[key] = get_market_outcome(slug, out)
        res = seen_slugs[key]
        if res == "WIN":
            whale_wins   += 1; whale_won_amt  += float(t.get("size") or 0)
        elif res == "LOSS":
            whale_losses += 1; whale_lost_amt += _val(t)
        else:
            whale_open   += 1; whale_open_amt += _val(t)
    return {
        "total_trades":   total,
        "avg_bet":        avg_bet,
        "big_bets":       big_bets,
        "low_odds_bets":  low_odds,
        "whale_count":    len(whale_trades),
        "whale_spent":    whale_spent,
        "whale_wins":     whale_wins,
        "whale_losses":   whale_losses,
        "whale_open":     whale_open,
        "whale_won_amt":  whale_won_amt,
        "whale_lost_amt": whale_lost_amt,
        "whale_open_amt": whale_open_amt,
    }


# ── TUI ────────────────────────────────────────────────────────────────────────

WIN_STYLE  = Style(bgcolor="rgb(5,46,22)")
LOSS_STYLE = Style(bgcolor="rgb(69,10,10)")
NEW_STYLE  = Style(bgcolor="rgb(30,58,138)")
HOF_STYLE  = Style(bgcolor="rgb(78,63,0)")


def _status_text(res: str) -> Text:
    if res == "WIN":  return Text("✓ WIN",  style="bold rgb(74,222,128)")
    if res == "LOSS": return Text("✗ LOSS", style="bold rgb(220,38,38)")
    if res == "VOID": return Text("~ VOID", style="dim")
    return Text("open", style="rgb(234,179,8)")


def _row_style(res: str, is_new: bool, is_hof: bool = False) -> Style:
    if res == "WIN":  return WIN_STYLE
    if res == "LOSS": return LOSS_STYLE
    if is_hof:        return HOF_STYLE
    if is_new:        return NEW_STYLE
    return Style()


class WhaleScannerApp(App):
    CSS = """
    Screen { layout: vertical; background: #0a0a0f; }
    #status-bar { height: 2; padding: 0 1; background: #111128; border-bottom: solid cyan; }
    #main-area { height: 1fr; layout: horizontal; }
    #table-box { width: 65%; height: 1fr; border: solid cyan; }
    #detail-box { width: 35%; height: 1fr; border: solid cyan; padding: 0 1; }
    DataTable { height: 1fr; }
    #detail-title { height: 1; background: #111128; padding: 0 1;
                    border-bottom: dashed cyan; color: cyan; }
    #detail-body { height: 1fr; padding: 1 1; overflow-y: auto; overflow-x: hidden; }
    Footer { background: #111128; }
    """
    BINDINGS = [
        Binding("q", "quit",           "Quit"),
        Binding("r", "force_poll",     "Poll now"),
        Binding("c", "clear_resolved", "Clear resolved"),
        Binding("h", "toggle_hof",     "★ HoF"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._seen:         set[str]        = set()
        self._pending:      list[dict]      = []
        self._rows:         dict[str, dict] = {}
        self._stats_cache:  dict[str, dict] = {}
        self._hof:          dict[str, dict] = _load_hof()
        self._last_poll     = 0.0
        self._last_scanned  = 0
        self._status_msg    = "Starting up..."
        self._startup_done  = False
        self._loading_tx:   str | None      = None
        self._selected_tx:  str | None      = None

    def compose(self) -> ComposeResult:
        yield Static("", id="status-bar")
        with Horizontal(id="main-area"):
            with Vertical(id="table-box"):
                yield DataTable(id="trades-table", cursor_type="row", zebra_stripes=False)
            with Vertical(id="detail-box"):
                yield Static("WHALE SCANNER  |  press h to add trader to HoF", id="detail-title")
                yield Static("← select a row", id="detail-body")
        yield Footer()

    async def on_mount(self) -> None:
        t = self.query_one("#trades-table", DataTable)
        t.add_column("Time",    key="time",    width=6)
        t.add_column("Market",  key="market",  width=34)
        t.add_column("Outcome", key="outcome", width=10)
        t.add_column("Odds",    key="odds",    width=6)
        t.add_column("Spent",   key="spent",   width=9)
        t.add_column("Wins",    key="wins",    width=9)
        t.add_column("Ret%",    key="ret",     width=5)
        t.add_column("Status",  key="status",  width=7)
        self.set_interval(1,         self._tick_status)
        self.set_interval(POLL_SECS, self._poll_live)
        self.set_interval(30,        self._poll_hof_wallets)
        self.set_interval(60,        self._check_pending)
        asyncio.create_task(self._startup())

    async def _startup(self) -> None:
        self._status_msg = "Seeding live feed..."
        try:
            initial = await asyncio.to_thread(fetch_trades)
            for t in initial:
                self._seen.add(t.get("transactionHash", ""))
        except Exception:
            pass
        self._startup_done = True
        self._last_poll    = time.time()

        self._status_msg = "Loading today's history..."
        all_hits = await asyncio.to_thread(fetch_today_history)
        for h in all_hits:
            self._add_row(h, is_new=False)
            if h.get("slug"):
                self._pending.append(h)
        self._status_msg = f"{len(all_hits)} today's bets loaded"
        asyncio.create_task(self._resolve_batch(list(self._pending)))

    async def _poll_live(self) -> None:
        if not self._startup_done:
            return
        try:
            trades = await asyncio.to_thread(fetch_trades)
            self._last_scanned = len(trades)
            candidates = [t for t in trades
                          if t.get("transactionHash", "") not in self._seen]
            for t in candidates:
                self._seen.add(t.get("transactionHash", ""))
            hits = await asyncio.to_thread(filter_trades, candidates)
            for h in hits:
                self._add_row(h, is_new=True)
                if h.get("slug"):
                    self._pending.append(h)
            self._last_poll = time.time()
        except Exception as e:
            self._status_msg = f"Poll error: {e}"

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

    def _add_row(self, h: dict, is_new: bool) -> None:
        if h["tx"] in self._rows:
            return
        table   = self.query_one("#trades-table", DataTable)
        implied = (1 / h["price"] - 1) * 100
        trade_t = datetime.fromtimestamp(h["ts"]).strftime("%H:%M") if h["ts"] else "?"
        market  = (h["title"][:32] + "…") if len(h["title"]) > 33 else h["title"]
        is_hof  = h.get("wallet", "") in self._hof
        rs      = _row_style(h.get("resolution", ""), is_new, is_hof)
        table.add_row(
            Text(trade_t,               style=rs),
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
        if is_new:
            table.scroll_end(animate=False)
            table.move_cursor(row=table.row_count - 1)

    def _update_row_style(self, h: dict) -> None:
        table   = self.query_one("#trades-table", DataTable)
        is_hof  = h.get("wallet", "") in self._hof
        rs      = _row_style(h.get("resolution", ""), False, is_hof)
        implied = (1 / h["price"] - 1) * 100
        trade_t = datetime.fromtimestamp(h["ts"]).strftime("%H:%M") if h["ts"] else "?"
        market  = (h["title"][:32] + "…") if len(h["title"]) > 33 else h["title"]
        try:
            table.update_cell(h["tx"], "time",    Text(trade_t,                style=rs))
            table.update_cell(h["tx"], "market",  Text(market,                 style=rs))
            table.update_cell(h["tx"], "outcome", Text(h["outcome"][:10],      style=rs))
            table.update_cell(h["tx"], "odds",    Text(f"{h['price']:.3f}",    style=Style.combine([rs, Style(color="cyan")])))
            table.update_cell(h["tx"], "spent",   Text(f"${h['spent']:,.0f}",  style=Style.combine([rs, Style(bold=True)])))
            table.update_cell(h["tx"], "wins",    Text(f"${h['payout']:,.0f}", style=rs))
            table.update_cell(h["tx"], "ret",     Text(f"{implied:.0f}%",      style=rs))
            table.update_cell(h["tx"], "status",  _status_text(h["resolution"]))
        except Exception:
            pass

    def _tick_status(self) -> None:
        sb    = self.query_one("#status-bar", Static)
        now   = datetime.now().strftime("%H:%M:%S")
        lag   = int(time.time() - self._last_poll) if self._last_poll else 0
        pulse = "·" * (int(time.time()) % 4 + 1)
        scan  = f"[dim]scanned {self._last_scanned}[/dim]" if self._last_scanned else "[dim]starting...[/dim]"
        extra = f"  [dim]{self._status_msg}[/dim]" if self._status_msg else ""
        sb.update(
            f"[bold cyan]Whale Scanner[/bold cyan] [dim]≥${MIN_SPEND} · ≤{MAX_PRICE:.0%} odds[/dim]  │  "
            f"[cyan]{pulse}[/cyan] {scan}  │  "
            f"{len(self._rows)} alerts  │  "
            f"[yellow]{len(self._pending)} pending[/yellow]  │  "
            f"[rgb(234,179,8)]★ {len(self._hof)} HoF[/rgb(234,179,8)]  │  {now}{extra}"
        )

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        tx = str(event.row_key.value)
        self._selected_tx = tx
        h  = self._rows.get(tx)
        if not h:
            return
        self._loading_tx = tx
        self._render_detail(h, stats=None, loading=bool(h.get("wallet")))
        if not h.get("wallet"):
            return
        if h["wallet"] in self._stats_cache:
            if self._loading_tx == tx:
                self._render_detail(h, self._stats_cache[h["wallet"]])
            return
        stats = await asyncio.to_thread(get_user_stats, h["wallet"])
        self._stats_cache[h["wallet"]] = stats
        if self._loading_tx == tx:
            self._render_detail(h, stats)

    def _render_detail(self, h: dict, stats: dict | None, loading: bool = False) -> None:
        body        = self.query_one("#detail-body", Static)
        implied     = (1 / h["price"] - 1) * 100
        res         = h.get("resolution", "")
        trade_t     = datetime.fromtimestamp(h["ts"]).strftime("%H:%M:%S") if h["ts"] else "?"
        market_url  = f"https://polymarket.com/event/{h['slug']}" if h.get("slug") else ""
        profile_url = f"https://polymarket.com/profile/{h['wallet']}" if h.get("wallet") else ""
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

        _add(_sep("BET"))
        t = Text(); t.append("Time   : ", C); t.append(f"{trade_t}   "); t.append(res_txt, res_sty); _add(t)
        _add(_kv("Outcome:", f"{h['outcome']}  @ {h['price']:.3f} odds  ({implied:.0f}% return)"))
        t = Text(); t.append("Spent  : ", C); t.append(f"${h['spent']:,.0f}", "bold")
        t.append("  →  wins "); t.append(f"${h['payout']:,.0f}", "bold"); t.append(" if hits"); _add(t)
        _add("")
        _add(_kv("Market :", h["title"]))
        if market_url:
            _add(_url("Link   :", market_url))

        if stats or h.get("wallet"):
            _add("")
            wallet    = h.get("wallet", "")
            in_hof    = wallet in self._hof
            hof_entry = self._hof.get(wallet, {})
            _add(_sep("★ HALL OF FAME TRADER" if in_hof else "TRADER"))
            nl = Text(); nl.append("Name   : ", C); nl.append(h["name"])
            if in_hof:
                nl.append("  ★", "bold rgb(234,179,8)")
                if hof_entry.get("added"):
                    nl.append(f"  (added {hof_entry['added']})", "dim")
            _add(nl)
            if profile_url:
                _add(_url("Profile:", profile_url))
            hint = Text()
            hint.append("Press ", "dim"); hint.append("h", BC)
            hint.append(" to remove from HoF" if in_hof else " to add to HoF", "dim"); _add(hint)
            if loading and not stats:
                t = Text(); t.append("loading stats...", "dim"); _add(t)

        if stats:
            _add(_kv("History:", f"{stats['total_trades']} trades  avg ${stats['avg_bet']:,.0f}/bet  "
                     f"big {stats['big_bets']}  low-odds {stats['low_odds_bets']}"))
            wc = stats["whale_count"]
            if wc:
                _add("")
                _add(_sep("WHALE TRACK RECORD"))
                _add(_kv("Bets   :", f"{wc} bets  spent ${stats['whale_spent']:,.0f}"))
                t = Text(); t.append("Won    : ", C)
                t.append(f"{stats['whale_wins']} bets  +${stats['whale_won_amt']:,.0f}", "bold rgb(74,222,128)"); _add(t)
                t = Text(); t.append("Lost   : ", C)
                t.append(f"{stats['whale_losses']} bets  -${stats['whale_lost_amt']:,.0f}", "bold rgb(220,38,38)"); _add(t)
                t = Text(); t.append("Open   : ", C)
                t.append(f"{stats['whale_open']} bets  ${stats['whale_open_amt']:,.0f} at risk", "rgb(234,179,8)"); _add(t)

        body.update(content)

    async def _poll_hof_wallets(self) -> None:
        """Poll each HoF wallet individually and surface trades that match the filter."""
        if not self._startup_done:
            return
        self._hof = await asyncio.to_thread(_load_hof)
        for wallet in list(self._hof.keys()):
            try:
                raw = await asyncio.to_thread(_fetch_wallet_recent, wallet)
            except Exception:
                continue
            candidates = [t for t in raw
                          if t.get("transactionHash", "") not in self._seen]
            for t in candidates:
                self._seen.add(t.get("transactionHash", ""))
            hits = await asyncio.to_thread(filter_trades, candidates)
            for h in hits:
                self._add_row(h, is_new=True)
                if h.get("slug"):
                    self._pending.append(h)

    async def action_force_poll(self) -> None:
        await self._poll_live()

    def action_clear_resolved(self) -> None:
        table = self.query_one("#trades-table", DataTable)
        for tx in [tx for tx, h in self._rows.items()
                   if h.get("resolution") in ("WIN", "LOSS", "VOID")]:
            try: table.remove_row(tx)
            except Exception: pass
            self._rows.pop(tx, None)

    def action_toggle_hof(self) -> None:
        if not self._selected_tx:
            return
        h = self._rows.get(self._selected_tx)
        if not h or not h.get("wallet"):
            return
        wallet = h["wallet"]
        if wallet in self._hof:
            del self._hof[wallet]
            self._status_msg = f"Removed {h['name']} from HoF"
        else:
            self._hof[wallet] = {
                "name":   h["name"],
                "added":  datetime.now().strftime("%Y-%m-%d %H:%M"),
                "wallet": wallet,
            }
            self._status_msg = f"★ Added {h['name']} to HoF"
        _save_hof(self._hof)
        stats = self._stats_cache.get(wallet)
        self._render_detail(h, stats)
        # Re-colour all rows for this wallet
        table = self.query_one("#trades-table", DataTable)
        for tx, row in self._rows.items():
            if row.get("wallet") == wallet:
                self._update_row_style(row)


if __name__ == "__main__":
    WhaleScannerApp().run()
