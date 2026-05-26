"""Kalshi BTC 15-min market consumer for the live prediction server.

Adapted from trading/kalshi/kalshi_btc_live_feed.py — same WS subscription,
same parity math (YES ask = 1 − NO bid), same 15-min rollover; writes to a
shared state object instead of printing.

Auth (KALSHI_API_KEY / KALSHI_API_SECRET) is loaded via dotenv from the repo
root's .env. If credentials are missing or the import fails, the consumer
logs a warning and yields control — the prediction server continues to run.

Also fetches market metadata (strike, type) via REST on each new subscription
so the page can show what the YES contract actually pays out on. YES is
"P(close > strike)" — only equals "P(close > open)" when strike ≈ open.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import websockets

log = logging.getLogger("btc_predictor.kalshi")

# btc-predictor/src/collector/kalshi_feed.py
#   parents[0] = collector/
#   parents[1] = src/
#   parents[2] = btc-predictor/
#   parents[3] = highlowticker-tui/
REPO_ROOT = Path(__file__).resolve().parents[3]
KALSHI_DIR = REPO_ROOT / "trading" / "kalshi"
sys.path.insert(0, str(KALSHI_DIR))

# Make sure .env vars are loaded BEFORE kalshi_auth is imported (it reads
# KALSHI_API_KEY / KALSHI_API_SECRET via os.environ[...] at import time).
try:
    from dotenv import load_dotenv  # type: ignore
    for candidate in (REPO_ROOT / ".env", REPO_ROOT / "highlow-tui" / ".env"):
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            log.info("loaded env from %s", candidate)
except Exception as e:  # noqa: BLE001
    log.debug("dotenv not loaded (%s)", e)

try:
    from kalshi_auth import KALSHI_WS, sign_headers  # type: ignore
    KALSHI_AVAILABLE = True
except Exception as e:  # noqa: BLE001
    KALSHI_AVAILABLE = False
    KALSHI_WS = None
    sign_headers = None
    log.warning("kalshi_auth unavailable (%s) — arb signals disabled", e)

ET = timezone(timedelta(hours=-4))

# Minimum size to count a price level as a "real" bid. Anything below this is
# treated as dust / stale phantom book entry and ignored when computing the
# best bid. Sourced from the working trader code in kalshi_lively_live.py.
PHANTOM_MIN_SIZE = 5

# yes_ask + no_ask should be ~1.0 in a healthy market. If it stays outside
# this band for more than ANOMALY_GRACE_SEC, the orderbook is corrupted by
# accumulated delta errors and needs a force-resubscribe.
ANOMALY_LOW = 0.95
ANOMALY_HIGH = 1.10
ANOMALY_GRACE_SEC = 5.0


def current_btc_ticker() -> str:
    """KXBTC15M-{YY}{MON}{DD}{HH}{MM}-{MM} for the current 15-min window."""
    now = datetime.now(timezone.utc)
    mins = (now.minute // 15) * 15
    ws_utc = now.replace(minute=mins, second=0, microsecond=0)
    close_utc = ws_utc + timedelta(minutes=15)
    close_et = close_utc.astimezone(ET)
    return f"KXBTC15M-{close_et.strftime('%y%b%d%H%M').upper()}-{close_et.strftime('%M')}"


@dataclass
class KalshiBookState:
    ticker: Optional[str] = None
    yes_bid: Optional[float] = None
    yes_ask: Optional[float] = None
    no_bid: Optional[float] = None
    no_ask: Optional[float] = None
    last_update_ms: Optional[int] = None
    available: bool = field(default=KALSHI_AVAILABLE)
    # Market metadata fetched via REST
    floor_strike: Optional[float] = None   # strike for "above" markets, or lower bound
    cap_strike: Optional[float] = None     # upper bound for "between" markets
    strike_type: Optional[str] = None      # "above", "below", "between", "functional"
    market_title: Optional[str] = None

    @property
    def yes_mid(self) -> Optional[float]:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return round((self.yes_bid + self.yes_ask) / 2.0, 4)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["yes_mid"] = self.yes_mid
        return d


def fetch_market_metadata(ticker: str) -> Optional[dict]:
    """GET /trade-api/v2/markets/{ticker} — returns strike, type, title, etc.

    Requires the same auth as the WS subscription. Returns None on any failure.
    """
    if not KALSHI_AVAILABLE:
        return None
    path = f"/trade-api/v2/markets/{ticker}"
    url = f"https://api.elections.kalshi.com{path}"
    try:
        headers = sign_headers("GET", path)
        req = urllib.request.Request(
            url,
            headers={**headers, "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data.get("market") or {}
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
        log.warning("Kalshi market metadata fetch failed for %s: %s", ticker, e)
        return None


async def kalshi_consumer(state: KalshiBookState) -> None:
    """Long-running task: keep `state` up to date with Kalshi orderbook."""
    if not KALSHI_AVAILABLE:
        log.info("Kalshi consumer disabled (no auth)")
        return  # task exits immediately

    delay = 1.0
    msg_id = 0

    def nid() -> int:
        nonlocal msg_id
        msg_id += 1
        return msg_id

    while True:
        try:
            log.info("Connecting to Kalshi WS")
            async with websockets.connect(
                KALSHI_WS,
                additional_headers=sign_headers("GET", "/trade-api/ws/v2"),
                ping_interval=20, open_timeout=10,
            ) as ws:
                delay = 1.0
                books: dict[str, dict[str, dict[float, float]]] = {}
                subscribed: set[str] = set()

                async def resubscribe() -> None:
                    tk = current_btc_ticker()
                    if tk in subscribed:
                        return
                    await ws.send(json.dumps({
                        "id": nid(), "cmd": "subscribe",
                        "params": {"channels": ["orderbook_delta"],
                                   "market_tickers": [tk]},
                    }))
                    subscribed.add(tk)
                    books[tk] = {"yes": {}, "no": {}}
                    log.info("subscribed to %s", tk)
                    # Fetch strike + market metadata once per ticker.
                    meta = await asyncio.to_thread(fetch_market_metadata, tk)
                    if meta:
                        try:
                            fs = meta.get("floor_strike")
                            cs = meta.get("cap_strike")
                            state.floor_strike = float(fs) if fs is not None else None
                            state.cap_strike = float(cs) if cs is not None else None
                            state.strike_type = meta.get("strike_type")
                            state.market_title = meta.get("title")
                            log.info(
                                "metadata %s: strike_type=%s floor=%s cap=%s",
                                tk, state.strike_type,
                                state.floor_strike, state.cap_strike,
                            )
                        except (TypeError, ValueError) as e:
                            log.debug("metadata parse error for %s: %s", tk, e)

                # Tickers flagged as anomalous — drop & re-subscribe to refresh.
                anomaly_first_seen: dict[str, float] = {}
                pending_resub: set[str] = set()

                async def force_resub_anomalies() -> None:
                    flagged = pending_resub & subscribed
                    if not flagged:
                        return
                    pending_resub.clear()
                    tks = list(flagged)
                    await ws.send(json.dumps({
                        "id": nid(), "cmd": "unsubscribe",
                        "params": {"channels": ["orderbook_delta"],
                                   "market_tickers": tks},
                    }))
                    subscribed.difference_update(tks)
                    for tk in tks:
                        books[tk] = {"yes": {}, "no": {}}
                        anomaly_first_seen.pop(tk, None)
                    log.warning("force-resubbing %d anomalous tickers: %s",
                                len(tks), ",".join(tks))

                async def rolling_subscribe() -> None:
                    while True:
                        try:
                            await force_resub_anomalies()
                            await resubscribe()
                        except Exception as e:  # noqa: BLE001
                            log.debug("resubscribe error: %s", e)
                        await asyncio.sleep(2)

                def _best_bid(side_book: dict[float, float]) -> Optional[float]:
                    """Highest price level that carries ≥ PHANTOM_MIN_SIZE size.
                    Falls back to overall max only if nothing qualifies."""
                    if not side_book:
                        return None
                    significant = [p for p, sz in side_book.items()
                                   if sz >= PHANTOM_MIN_SIZE]
                    if significant:
                        return max(significant)
                    any_p = [p for p, sz in side_book.items() if sz > 0]
                    return max(any_p) if any_p else None

                def publish(tk: str) -> None:
                    bk = books.get(tk) or {"yes": {}, "no": {}}
                    yb = _best_bid(bk["yes"])
                    nb = _best_bid(bk["no"])
                    if yb is None or nb is None:
                        return
                    ya = round(1.0 - nb, 4)
                    na = round(1.0 - yb, 4)
                    # Reject nonsense values
                    if not (0.001 <= ya <= 0.999) or not (0.001 <= na <= 0.999):
                        return
                    # Crossed-market / phantom-entry detection
                    sm = ya + na
                    if sm < ANOMALY_LOW or sm > ANOMALY_HIGH:
                        now_t = datetime.now(timezone.utc).timestamp()
                        first = anomaly_first_seen.setdefault(tk, now_t)
                        if now_t - first > ANOMALY_GRACE_SEC:
                            pending_resub.add(tk)
                            log.warning(
                                "anomaly %s yb=%.3f nb=%.3f → ya=%.3f na=%.3f "
                                "Σ=%.3f (out of [%.2f,%.2f]); flagging resub",
                                tk, yb, nb, ya, na, sm, ANOMALY_LOW, ANOMALY_HIGH,
                            )
                        return  # don't publish bad data
                    anomaly_first_seen.pop(tk, None)
                    state.ticker = tk
                    state.yes_bid = round(yb, 4)
                    state.yes_ask = ya
                    state.no_bid = round(nb, 4)
                    state.no_ask = na
                    state.last_update_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                await resubscribe()
                roll_task = asyncio.create_task(rolling_subscribe())
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        mtype = msg.get("type")
                        d = msg.get("msg", msg)
                        tk = d.get("market_ticker")
                        if not tk or tk not in books:
                            continue
                        if mtype == "orderbook_snapshot":
                            bk: dict[str, dict[float, float]] = {"yes": {}, "no": {}}
                            for side, key in (("yes", "yes_dollars_fp"),
                                              ("no", "no_dollars_fp")):
                                for entry in (d.get(key) or []):
                                    try:
                                        p = round(float(entry[0]), 4)
                                        sz = float(entry[1])
                                        if sz > 0:
                                            bk[side][p] = sz
                                    except (ValueError, TypeError, IndexError):
                                        continue
                            books[tk] = bk
                            publish(tk)
                        elif mtype == "orderbook_delta":
                            side = d.get("side")
                            if side not in ("yes", "no"):
                                continue
                            try:
                                price = round(float(d.get("price_dollars", 0)), 4)
                                delta = float(d.get("delta_fp", 0))
                            except (ValueError, TypeError):
                                continue
                            bk2 = books.setdefault(tk, {"yes": {}, "no": {}})
                            new_sz = bk2[side].get(price, 0) + delta
                            if new_sz <= 0:
                                bk2[side].pop(price, None)
                            else:
                                bk2[side][price] = new_sz
                            publish(tk)
                finally:
                    roll_task.cancel()
                    try:
                        await roll_task
                    except (asyncio.CancelledError, Exception):
                        pass

        except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as e:
            log.warning("Kalshi WS error: %s — retry in %.0fs", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)


# ───────────────────── Arb signal computation ─────────────────────

DEFAULT_ARB_THRESHOLDS = {
    "k_yes": 0.45,   # Kalshi YES mid must exceed this for BUY YES to fire
    "k_no":  0.45,   # Kalshi NO  mid must exceed this for BUY NO  to fire
    "d_yes": 0.12,   # (Model P(YES) − Kalshi P(YES)) must exceed this
    "d_no":  0.12,   # (Model P(NO)  − Kalshi P(NO))  must exceed this
}
ARB_MIN_SAMPLE_SEC = 30     # don't fire during first 30s of a window —
                            # TWAP / flow features need time to settle


def compute_arb_signal(pred: dict, kalshi: dict,
                       thresholds: Optional[dict] = None) -> Optional[dict]:
    """Two symmetric triggers, emitting at most one signal per tick.

    Thresholds are configurable (the server can auto-tune them from history);
    defaults are DEFAULT_ARB_THRESHOLDS.

    BUY YES   when (Model_P_YES − Kalshi_P_YES) > d_yes  AND  Kalshi_P_YES > k_yes
    BUY NO    when (Model_P_NO  − Kalshi_P_NO)  > d_no   AND  Kalshi_P_NO  > k_no

    Gating on the KALSHI side filters out deep-OTM trades.
    """
    th = thresholds or DEFAULT_ARB_THRESHOLDS
    yes_bid = kalshi.get("yes_bid")
    yes_ask = kalshi.get("yes_ask")
    if yes_bid is None or yes_ask is None:
        return None
    if pred.get("sample_sec", 0) < ARB_MIN_SAMPLE_SEC:
        return None
    yes_mid = round((yes_bid + yes_ask) / 2.0, 4)

    model_p_yes = pred["p_green"]
    model_p_no  = 1.0 - model_p_yes
    kalshi_p_no = 1.0 - yes_mid

    delta_yes = model_p_yes - yes_mid
    delta_no  = model_p_no  - kalshi_p_no

    base = {
        "model_p_up": round(model_p_yes, 4),
        "yes_mid":    yes_mid,
        "yes_bid":    yes_bid,
        "yes_ask":    yes_ask,
    }
    if delta_yes > th["d_yes"] and yes_mid > th["k_yes"]:
        return {
            **base,
            "direction":   "BUY YES",
            "side":        "YES",
            "delta_pp":    round(delta_yes, 4),
            "model_p":     round(model_p_yes, 4),
            "market_mid":  yes_mid,
            "market_bid":  yes_bid,
            "market_ask":  yes_ask,
        }
    if delta_no > th["d_no"] and kalshi_p_no > th["k_no"]:
        no_bid = round(1.0 - yes_ask, 4)
        no_ask = round(1.0 - yes_bid, 4)
        return {
            **base,
            "direction":   "BUY NO",
            "side":        "NO",
            "delta_pp":    round(delta_no, 4),
            "model_p":     round(model_p_no, 4),
            "market_mid":  round(kalshi_p_no, 4),
            "market_bid":  no_bid,
            "market_ask":  no_ask,
        }
    return None


# ─────────── Hindsight simulation + grid search ────────────
# Shared by the /optimal_settings endpoint AND the auto-tune loop in
# on_window_settled. Single implementation = one place to fix any
# trade-mechanics bugs.

K_GRID = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
D_GRID = [0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20]

# Same-side cooldown in seconds. Set to 3 to match the cadence at which we
# actually click buy when a signal stays continuously hot — 5s was undercounting
# real fills (e.g. the 19:15 window: 10s of continuous signal counted as 2
# trades; in reality we executed 4).
ARB_THROTTLE_SEC = 3


def simulate_window(rows: list[dict], yes_won: bool,
                    k_yes: float, k_no: float, d_yes: float, d_no: float,
                    min_sec: int = ARB_MIN_SAMPLE_SEC,
                    throttle_sec: int = ARB_THROTTLE_SEC) -> tuple[float, int, int, int]:
    """Replay one window's per-second rows. Returns the totals tuple
    `(total_pnl, n_trades, n_wins, n_losses)`. Hot path for the grid
    search — keep the tuple return so 2,401-combo sweeps stay cheap.
    For richer per-side breakdown call `simulate_window_breakdown`.
    """
    pnl, n, won, lost, *_ = _simulate_window_full(
        rows, yes_won, k_yes, k_no, d_yes, d_no, min_sec, throttle_sec,
    )
    return pnl, n, won, lost


def simulate_window_breakdown(rows: list[dict], yes_won: bool,
                              k_yes: float, k_no: float, d_yes: float, d_no: float,
                              min_sec: int = ARB_MIN_SAMPLE_SEC,
                              throttle_sec: int = ARB_THROTTLE_SEC) -> dict:
    """Same simulation as `simulate_window`, but returns a dict with
    per-outcome averages and per-side counts — used at settlement time so
    the dashboard can show `wins × avg_winner_entry / losses × avg_loser_entry`
    instead of just trade counts.
    """
    pnl, n, won, lost, win_sum, loss_sum, yes_n, no_n = _simulate_window_full(
        rows, yes_won, k_yes, k_no, d_yes, d_no, min_sec, throttle_sec,
    )
    return {
        "pnl":               pnl,
        "n":                 n,
        "wins":              won,
        "losses":            lost,
        "yes_n":             yes_n,
        "no_n":              no_n,
        "winner_avg_entry":  (win_sum / won) if won else None,
        "loser_avg_entry":   (loss_sum / lost) if lost else None,
    }


def _simulate_window_full(rows, yes_won, k_yes, k_no, d_yes, d_no, min_sec, throttle_sec):
    """Inner loop. Returns the extended tuple consumed by both public
    wrappers above. Defined as a free function so neither wrapper pays
    a method-dispatch hit.
    """
    pnl = 0.0
    n = won = lost = 0
    yes_n = no_n = 0
    win_sum = 0.0      # sum of winning entries (for avg)
    loss_sum = 0.0     # sum of losing entries  (for avg)
    last_y, last_n = -10_000, -10_000
    for r in rows:
        s = r.get("sample_sec")
        if s is None or s < min_sec:
            continue
        yb = r.get("kalshi_yes_bid"); ya = r.get("kalshi_yes_ask")
        pg = r.get("p_green")
        if yb is None or ya is None or pg is None:
            continue
        ym = (yb + ya) / 2.0; nm = 1.0 - ym
        pn = 1.0 - pg
        dy = pg - ym; dn = pn - nm
        if dy > d_yes and ym > k_yes and s - last_y >= throttle_sec:
            entry = ya
            payoff = 1.0 if yes_won else 0.0
            pnl += payoff - entry
            n += 1; yes_n += 1
            if yes_won: won += 1; win_sum += entry
            else:       lost += 1; loss_sum += entry
            last_y = s
            continue
        if dn > d_no and nm > k_no and s - last_n >= throttle_sec:
            entry = 1.0 - yb   # NO_ask
            payoff = 1.0 if not yes_won else 0.0
            pnl += payoff - entry
            n += 1; no_n += 1
            if not yes_won: won += 1; win_sum += entry
            else:           lost += 1; loss_sum += entry
            last_n = s
    return pnl, n, won, lost, win_sum, loss_sum, yes_n, no_n


def find_optimal_thresholds(window_data: list[tuple[list[dict], bool]]
                            ) -> Optional[dict]:
    """Grid-search the parameter space against a list of (rows, yes_won)
    tuples for completed windows. Returns the threshold dict whose total
    P&L is highest. Ties are broken in favour of the most conservative
    setting (highest K and Δ) — when many combos extract the same P&L on
    thin data, the tightest filter is the least overfit one.
    """
    if not window_data:
        return None
    # Round to 6 dp so float jitter doesn't fragment ties.
    scored: list[tuple[float, float, float, float, float]] = []
    for ky in K_GRID:
        for kn in K_GRID:
            for dy in D_GRID:
                for dn in D_GRID:
                    total = 0.0
                    for rows, yw in window_data:
                        pnl, *_ = simulate_window(rows, yw, ky, kn, dy, dn)
                        total += pnl
                    scored.append((round(total, 6), ky, kn, dy, dn))
    # Sort: highest P&L first; on tie, prefer larger K_yes, K_no, Δ_yes, Δ_no.
    scored.sort(key=lambda r: (-r[0], -r[1], -r[2], -r[3], -r[4]))
    _, ky, kn, dy, dn = scored[0]
    return {"k_yes": ky, "k_no": kn, "d_yes": dy, "d_no": dn}
