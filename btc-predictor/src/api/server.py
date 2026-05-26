"""FastAPI server: consumes Coinbase Advanced Trade WS, runs the trained
LightGBM model, broadcasts predictions to browser clients."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from src.collector.coinbase_backfill import backfill_engine
from src.collector.kalshi_feed import (
    DEFAULT_ARB_THRESHOLDS,
    KalshiBookState,
    compute_arb_signal,
    find_optimal_thresholds,
    kalshi_consumer,
    simulate_window,
    simulate_window_breakdown,
)
from src.model.live_engine import (
    ALL_FEATURE_NAMES,
    LiveFeatureEngine,
    parse_iso_to_ns,
)
from src.api.kalshi_executor import (
    LIVE_TRADING_ENABLED,
    place_arb_buy,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = REPO_ROOT / "src" / "model" / "model.pkl"
BASELINES_PATH = REPO_ROOT / "data" / "processed" / "baselines.json"
HTML_PATH = REPO_ROOT / "web" / "live_monitor.html"
# Append-only JSONL log of (wall_ts, model P, Kalshi prices) at 1Hz dedupe
# so we can backtest / chart trends across many windows after the fact.
PREDICTION_LOG_PATH = REPO_ROOT / "data" / "predictions.jsonl"
# Append-only ledger of every settled arb trade (one line per trade, written
# when it flips to won/lost). Source of truth for "what trades did we take?"
ARB_TRADES_LOG_PATH = REPO_ROOT / "data" / "arb_trades.jsonl"
# Append-only per-window settlement summary (one line per settled window).
# Source of truth for the dashboard's settlement history table and the
# auto-tuner's lookback. Includes the thresholds active at settle time.
WINDOW_PNL_LOG_PATH = REPO_ROOT / "data" / "window_pnl.jsonl"

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
PREDICTION_HZ = 5.0   # broadcasts per second (browser is the sole renderer)
# How many recent windows to backfill into the dashboard settlement table on
# startup. Decoupled from AUTOTUNE_WINDOW_LOOKBACK so the table can be deeper
# than the auto-tune horizon.
REHYDRATE_HISTORY_DEPTH = 20

log = logging.getLogger("btc_predictor.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _append_jsonl(path: Path, record: dict) -> None:
    """Append one line to a JSONL file, creating parents if needed.
    Swallowing the OSError keeps the live loop alive — we'd rather lose a
    log line than crash the tracker."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as e:
        log.warning("append to %s failed: %s", path.name, e)


def _determine_outcome(open_px: float | None, close_px: float | None,
                       kalshi_yes_mid: float | None,
                       last_sample_sec: int | None = None) -> bool:
    """Decide whether the YES contract paid out (i.e. UP won).

    Kalshi's actual settlement uses their BRTI index against their published
    strike — that doesn't always match `close_px > open_px` for our TWAP
    approximation when the move is < 1 bps. Empirically, 2.8% of windows
    showed our TWAP-based call disagreeing with Kalshi's market verdict.

    Use Kalshi's own market quote at settle as ground truth: YES_mid → 1.0
    means YES paid out, YES_mid → 0.0 means NO paid out. By the last few
    seconds of the window the market reliably converges to the true outcome.

    Fallback to `close_px > open_px` only when Kalshi data is missing or
    the late-window tick wasn't recorded.
    """
    # If we have a clear late-window Kalshi quote, trust it over TWAP.
    if (kalshi_yes_mid is not None
        and last_sample_sec is not None and last_sample_sec >= 870):
        return kalshi_yes_mid > 0.5
    # Otherwise, fall back to TWAP comparison.
    if open_px is not None and close_px is not None:
        return close_px > open_px
    return False  # safest default — treat as DOWN if all signals missing


# Same-side cooldown for live arb trades, in seconds. Matches
# ARB_THROTTLE_SEC in simulate_window so the live trade count equals what
# the settlement backtest records.
LIVE_ARB_THROTTLE_SEC = 3


class ArbTradeTracker:
    """Server-side authority for arb trade decisions and P&L.

    Each call to `process_tick(pred, arb)` either:
      - appends a new trade (if arb fired AND throttle allows)
      - updates live mark-to-market P&L on every existing trade
      - flips state to "settled" when the trade's window has rolled

    The dashboard simply renders whatever `as_payload()` returns — no
    decision logic lives in JS.
    """
    def __init__(self):
        # Trades in the currently-active window (resets on roll). Each:
        #  {sample_sec, wall_ts, direction, entry_ask, entry_bid,
        #   market_mid, model_p, delta_pp, pnl, status, window_open_ts}
        self.trades: list[dict] = []
        self.current_window_ts: float | None = None
        # Per-window snapshots so we can settle trades whose window
        # already rolled. Keyed by window_open_ts → {open_px, close_px}.
        self.window_finals: dict[float, dict] = {}
        # Per-side throttle on sample_sec (matches simulate_window).
        self._last_yes_sec: int = -10_000
        self._last_no_sec:  int = -10_000
        # Live-trading: track in-flight orders so we don't fire a 2nd order
        # on the same side while the 1st is still resolving (3-5s window).
        self._yes_order_pending: bool = False
        self._no_order_pending:  bool = False

    def process_tick(self, pred: dict, arb: dict | None) -> None:
        wts = pred.get("window_open_ts")
        sec = pred.get("sample_sec")
        cur_px = pred.get("current_price")
        opx = pred.get("window_open_px")

        # Window roll → settle the prior window's still-live trades to disk
        # (so the per-trade ledger captures them), then reset the live list.
        # Without this step trades would be dropped before _refresh_pnl could
        # see them in their "settle once the window has rolled" branch.
        if self.current_window_ts is not None and wts != self.current_window_ts:
            prev = self.current_window_ts
            fin = self.window_finals.get(prev)
            settled = 0
            if fin and fin.get("open_px") is not None and fin.get("close_px") is not None:
                # Ground truth = Kalshi YES_mid at last tick (not TWAP).
                yes_won = _determine_outcome(
                    fin.get("open_px"), fin.get("close_px"),
                    fin.get("kalshi_yes_mid"), fin.get("sample_sec"),
                )
                import time as _t
                now_ts = round(_t.time(), 3)
                for t in self.trades:
                    if t["status"] in ("won", "lost"):
                        continue
                    entry = t["entry_ask"]
                    is_buy_yes = t["direction"] == "BUY YES"
                    correct = (is_buy_yes and yes_won) or (not is_buy_yes and not yes_won)
                    t["pnl"] = round((1.0 if correct else 0.0) - entry, 4)
                    t["status"] = "won" if correct else "lost"
                    _append_jsonl(ARB_TRADES_LOG_PATH, {
                        **t,
                        "settled_at_ts":    now_ts,
                        "outcome_close_px": fin["close_px"],
                        "outcome_open_px":  fin["open_px"],
                        "outcome_kalshi_yes_mid": fin.get("kalshi_yes_mid"),
                    })
                    settled += 1
            log.info("[arb-tracker] window rolled %s → %s, settled %d trades to ledger",
                     prev, wts, settled)
            self.trades = []
            self._last_yes_sec = -10_000
            self._last_no_sec  = -10_000
        self.current_window_ts = wts

        # Cache the latest snapshot of the current window so we can use it
        # to settle the trades after the roll. Includes the Kalshi YES_mid
        # so we can match Kalshi's actual settlement, not just our TWAP.
        if wts is not None and opx is not None and cur_px is not None:
            k = pred.get("kalshi") or {}
            self.window_finals[wts] = {
                "open_px":         opx,
                "close_px":        cur_px,
                "sample_sec":      sec,
                "kalshi_yes_mid":  k.get("yes_mid"),
            }

        # Maybe append a new trade.
        if arb is not None and sec is not None:
            direction = arb.get("direction")
            if direction == "BUY YES" and sec - self._last_yes_sec >= LIVE_ARB_THROTTLE_SEC:
                # Block if a previous live order on this side is still
                # resolving — avoids stacking unfilled orders.
                if LIVE_TRADING_ENABLED and self._yes_order_pending:
                    pass    # silently skip; throttle re-arms next tick
                else:
                    self._append_trade(pred, arb)
                    self._last_yes_sec = sec
                    if LIVE_TRADING_ENABLED:
                        self._spawn_live_order(pred, arb, "yes")
            elif direction == "BUY NO" and sec - self._last_no_sec >= LIVE_ARB_THROTTLE_SEC:
                if LIVE_TRADING_ENABLED and self._no_order_pending:
                    pass
                else:
                    self._append_trade(pred, arb)
                    self._last_no_sec = sec
                    if LIVE_TRADING_ENABLED:
                        self._spawn_live_order(pred, arb, "no")

        # Refresh P&L on every existing trade (live mark-to-market AND
        # settle anything whose window has rolled).
        self._refresh_pnl(pred)

    def _append_trade(self, pred: dict, arb: dict) -> None:
        import time as _t, uuid as _uuid
        self.trades.append({
            "trade_id":       _uuid.uuid4().hex,    # stable ID, used to link
                                                    # live order results back
                                                    # to this paper record
            "wall_ts":        round(_t.time(), 3),
            "sample_sec":     pred.get("sample_sec"),
            "window_open_ts": pred.get("window_open_ts"),
            "direction":      arb.get("direction"),
            "entry_ask":      arb.get("market_ask"),
            "entry_bid":      arb.get("market_bid"),
            "market_mid":     arb.get("market_mid"),
            "model_p":        arb.get("model_p"),
            "delta_pp":       arb.get("delta_pp"),
            "pnl":            0.0,
            "status":         "live",   # live → won/lost on settle
            # Live-trading fields populated by _live_order_task when the
            # async order resolves. Stay None in paper mode.
            "execution_mode": "live" if LIVE_TRADING_ENABLED else "paper",
            "execution_path": None,
            "kalshi_order_id": None,
            "actual_fill_price": None,
            "fee_dollars":    None,
        })

    def _spawn_live_order(self, pred: dict, arb: dict, side: str) -> None:
        """Fire-and-forget the real Kalshi order placement. Patches the
        in-memory trade record when it completes. Marks the side as pending
        so concurrent fires get blocked."""
        k = pred.get("kalshi") or {}
        ticker = k.get("ticker")
        if not ticker:
            log.warning("[arb-tracker] no ticker in kalshi state — skipping live order")
            return
        snapshot = {
            "yes_bid": k.get("yes_bid"),
            "yes_ask": k.get("yes_ask"),
            "ts":      pred.get("wall_ts", __import__("time").time()),
        }
        trade_id = self.trades[-1]["trade_id"]
        tag = f"arb@T{pred.get('sample_sec')}"
        if side == "yes": self._yes_order_pending = True
        else:             self._no_order_pending = True
        asyncio.create_task(
            self._live_order_task(ticker, arb["direction"], snapshot, tag, trade_id, side)
        )

    async def _live_order_task(self, ticker: str, direction: str, snapshot: dict,
                                tag: str, trade_id: str, side: str) -> None:
        try:
            client = STATE.order_client
            if client is None:
                log.warning("[arb-tracker] order_client not initialized — skip")
                return
            result = await place_arb_buy(client, ticker, direction, snapshot, tag, trade_id)
            # Patch the in-memory trade record by trade_id (linear scan;
            # self.trades is bounded by trades-per-window so this is cheap).
            for t in self.trades:
                if t.get("trade_id") == trade_id:
                    t["execution_path"]    = result.get("execution_path")
                    t["kalshi_order_id"]   = result.get("kalshi_order_id") or result.get("kalshi_market_order_id")
                    t["actual_fill_price"] = result.get("actual_fill_price")
                    t["fee_dollars"]       = result.get("fee_dollars")
                    # If we got a real fill, override entry_ask with the
                    # actual fill price so settle P&L uses the real cost.
                    if result.get("actual_fill_price") is not None:
                        t["entry_ask"] = result["actual_fill_price"]
                    break
        finally:
            if side == "yes": self._yes_order_pending = False
            else:             self._no_order_pending = False

    def _refresh_pnl(self, pred: dict) -> None:
        cur_wts = pred.get("window_open_ts")
        k = pred.get("kalshi") or {}
        yb = k.get("yes_bid"); ya = k.get("yes_ask")
        for t in self.trades:
            if t["status"] in ("won", "lost"):
                continue
            entry = t["entry_ask"]
            is_buy_yes = t["direction"] == "BUY YES"
            same_window = (t["window_open_ts"] == cur_wts)
            if same_window and yb is not None and ya is not None:
                # Mark-to-market with the bid on the traded side.
                traded_bid = yb if is_buy_yes else (1.0 - ya)
                t["pnl"] = round(traded_bid - entry, 4)
                t["status"] = "live"
            elif not same_window:
                fin = self.window_finals.get(t["window_open_ts"])
                if fin and fin.get("open_px") is not None and fin.get("close_px") is not None:
                    yes_won = _determine_outcome(
                        fin.get("open_px"), fin.get("close_px"),
                        fin.get("kalshi_yes_mid"), fin.get("sample_sec"),
                    )
                    correct = (is_buy_yes and yes_won) or (not is_buy_yes and not yes_won)
                    t["pnl"] = round((1.0 if correct else 0.0) - entry, 4)
                    t["status"] = "won" if correct else "lost"
                    # Persist the settled trade so the ledger survives restart.
                    _append_jsonl(ARB_TRADES_LOG_PATH, {
                        **t,
                        "settled_at_ts":          round(__import__("time").time(), 3),
                        "outcome_close_px":       fin["close_px"],
                        "outcome_open_px":        fin["open_px"],
                        "outcome_kalshi_yes_mid": fin.get("kalshi_yes_mid"),
                    })

    def as_payload(self) -> dict:
        n = len(self.trades)
        total_pnl = round(sum(t["pnl"] for t in self.trades), 4)
        won  = sum(1 for t in self.trades if t["status"] == "won")
        lost = sum(1 for t in self.trades if t["status"] == "lost")
        live = sum(1 for t in self.trades if t["status"] == "live")

        # Per-side breakdown. Avg entry is the average price we paid on
        # entry for that side (BUY YES → YES_ask; BUY NO → NO_ask).
        yes_trades = [t for t in self.trades if t["direction"] == "BUY YES"]
        no_trades  = [t for t in self.trades if t["direction"] == "BUY NO"]
        def _side_stats(side_trades):
            if not side_trades:
                return {"n": 0, "won": 0, "lost": 0, "live": 0,
                        "avg_entry": None, "pnl": 0.0}
            return {
                "n":          len(side_trades),
                "won":        sum(1 for t in side_trades if t["status"] == "won"),
                "lost":       sum(1 for t in side_trades if t["status"] == "lost"),
                "live":       sum(1 for t in side_trades if t["status"] == "live"),
                "avg_entry":  round(sum(t["entry_ask"] for t in side_trades) / len(side_trades), 4),
                "pnl":        round(sum(t["pnl"] for t in side_trades), 4),
            }
        return {
            "window_open_ts": self.current_window_ts,
            "trades": list(self.trades),
            "n": n, "won": won, "lost": lost, "live": live,
            "total_pnl": total_pnl,
            "buy_yes":  _side_stats(yes_trades),
            "buy_no":   _side_stats(no_trades),
        }


class AppState:
    def __init__(self):
        self.model = None
        self.feature_cols: list[str] = list(ALL_FEATURE_NAMES)
        self.engine: LiveFeatureEngine | None = None
        self.kalshi: KalshiBookState = KalshiBookState()
        self.subscribers: set[WebSocket] = set()
        self.latest_prediction: dict | None = None
        self.last_trade_ts_ns: int | None = None
        # Per-window prediction history at 1-second resolution. Resets at
        # each 15-min boundary. Replayed to new WS subscribers on connect
        # so they can paint the chart back to sec 0.
        self.history: list[dict] = []
        self.history_window_ts: float | None = None
        # Append-only JSONL log to disk — survives restarts. One line per
        # (window_open_ts, sample_sec) so trends can be analyzed offline.
        self.log_fh = None
        self.last_log_key: tuple | None = None
        # Dynamic arb thresholds — auto-tune to the optimum of last 10
        # completed windows once enough history exists.
        self.arb_thresholds: dict = dict(DEFAULT_ARB_THRESHOLDS)
        # Track the currently-active window so we can fire on_window_settled
        # exactly once when it rolls.
        self.current_window_ts: float | None = None
        # Per-window settlement log — printed to stdout AND broadcast to the
        # dashboard. Keeps only the last 100 windows.
        self.window_pnl_log: list[dict] = []
        # Live arb trade authority — appends, marks-to-market, settles.
        self.arb_tracker: ArbTradeTracker = ArbTradeTracker()
        # httpx.AsyncClient for live Kalshi order placement. Only created
        # in lifespan() when LIVE_TRADING_ENABLED is set; stays None in
        # paper mode (default).
        self.order_client = None
        # Walk-forward backtest on 14+ windows showed last-5 lookback dominates:
        # higher hit rate (75% vs 52% defaults), survives drop-best-window
        # sensitivity (+$9.80 trimmed), and captures regime drift without
        # diluting on stale history. See follow-strategy comparison notes.
        self.AUTOTUNE_AFTER_N_WINDOWS = 5
        self.AUTOTUNE_WINDOW_LOOKBACK = 5


STATE = AppState()


async def coinbase_consumer():
    delay = 1.0
    while True:
        try:
            log.info("Connecting to %s", COINBASE_WS)
            async with websockets.connect(COINBASE_WS, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": [PRODUCT_ID],
                    "channel": "market_trades",
                }))
                log.info("Subscribed to market_trades for %s", PRODUCT_ID)
                delay = 1.0
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("channel") != "market_trades":
                        continue
                    for ev in msg.get("events", []):
                        for t in ev.get("trades", []):
                            try:
                                ts_ns = parse_iso_to_ns(t["time"])
                                price = float(t["price"])
                                qty = float(t["size"])
                                side = (t.get("side") or "").upper()
                                aggressor_buy = side == "BUY"
                                STATE.engine.process_tick(ts_ns, price, qty, aggressor_buy)
                                STATE.last_trade_ts_ns = ts_ns
                            except (KeyError, ValueError, TypeError) as e:
                                log.debug("bad trade msg: %s (%s)", t, e)
        except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as e:
            log.warning("Coinbase WS disconnected: %s — retrying in %.0fs", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)


async def prediction_publisher():
    interval = 1.0 / PREDICTION_HZ
    while True:
        await asyncio.sleep(interval)
        if STATE.engine is None or STATE.model is None:
            continue
        pred = STATE.engine.predict_now(STATE.model, STATE.feature_cols)
        if pred is None:
            continue
        kalshi_dict = STATE.kalshi.to_dict()
        pred["kalshi"] = kalshi_dict
        arb = compute_arb_signal(pred, kalshi_dict, STATE.arb_thresholds)
        if arb is not None:
            pred["arb"] = arb
        # Always ship the active thresholds so the dashboard header reflects
        # the current rule (which may have been auto-tuned away from defaults).
        pred["arb_thresholds"] = dict(STATE.arb_thresholds)
        # Server-side arb tracker: appends fresh trades, marks-to-market,
        # settles when window rolls. Dashboard renders this payload directly.
        STATE.arb_tracker.process_tick(pred, arb)
        pred["arb_window"] = STATE.arb_tracker.as_payload()
        STATE.latest_prediction = pred

        # Detect window roll → schedule on_window_settled for the window
        # that just completed. Runs once per boundary.
        w_ts = pred.get("window_open_ts")
        if STATE.current_window_ts is not None and STATE.current_window_ts != w_ts:
            asyncio.create_task(on_window_settled(STATE.current_window_ts))
        STATE.current_window_ts = w_ts

        # Maintain per-window history at 1Hz, but ONLY the chart-essential
        # fields. The browser uses history to repaint the chart on connect;
        # everything else (features, SHAP, candle-meta panel, action tile)
        # populates from the very next live message. Keeping history slim
        # avoids clogging the WS send buffer on initial connect (the full
        # payload is ~5 KB, slim is ~80 B — 60× smaller).
        w_ts = pred.get("window_open_ts")
        if STATE.history_window_ts != w_ts:
            STATE.history = []
            STATE.history_window_ts = w_ts
        kalshi_yes_mid = (pred.get("kalshi") or {}).get("yes_mid")
        slim = {
            "sample_sec":     pred.get("sample_sec"),
            "p_green":        pred.get("p_green"),
            "window_open_ts": w_ts,
            "kalshi":         {"yes_mid": kalshi_yes_mid} if kalshi_yes_mid is not None else None,
        }
        sec = pred.get("sample_sec")
        if STATE.history and STATE.history[-1].get("sample_sec") == sec:
            STATE.history[-1] = slim
        else:
            STATE.history.append(slim)

        # Persist to disk JSONL once per (window, second) so we can chart
        # trends across days. Skip if file isn't open (e.g. permission error).
        if STATE.log_fh is not None:
            log_key = (w_ts, sec)
            if STATE.last_log_key != log_key:
                STATE.last_log_key = log_key
                try:
                    import time as _t
                    k = kalshi_dict or {}
                    STATE.log_fh.write(json.dumps({
                        "wall_ts":         round(_t.time(), 3),
                        "window_open_ts":  w_ts,
                        "sample_sec":      sec,
                        "p_green":         pred.get("p_green"),
                        "strength":        pred.get("strength"),
                        "signal_strength": pred.get("signal_strength"),
                        "body_pct":        pred.get("body_pct"),
                        "current_price":   pred.get("current_price"),
                        "window_open_px":  pred.get("window_open_px"),
                        "kalshi_ticker":   k.get("ticker"),
                        "kalshi_yes_bid":  k.get("yes_bid"),
                        "kalshi_yes_ask":  k.get("yes_ask"),
                        "kalshi_yes_mid":  k.get("yes_mid"),
                    }) + "\n")
                    STATE.log_fh.flush()
                except (OSError, ValueError) as e:
                    log.warning("prediction-log write failed: %s", e)
                    try:
                        STATE.log_fh.close()
                    except Exception:
                        pass
                    STATE.log_fh = None

        # Broadcast to all subscribers concurrently with a per-send timeout.
        # Without this, one slow subscriber (e.g. a backgrounded browser tab
        # whose WS receive buffer has filled up) stalls the entire publisher
        # loop — all subscribers then appear "frozen" even though the engine
        # is producing predictions on schedule.
        async def _send(ws: WebSocket) -> WebSocket | None:
            try:
                await asyncio.wait_for(ws.send_json(pred), timeout=0.4)
                return None
            except Exception:
                return ws

        if STATE.subscribers:
            results = await asyncio.gather(
                *(_send(ws) for ws in list(STATE.subscribers)),
                return_exceptions=False,
            )
            dead = {ws for ws in results if ws is not None}
            if dead:
                STATE.subscribers -= dead
                log.info("Dropped %d slow/dead subscriber(s) — %d remain",
                         len(dead), len(STATE.subscribers))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    log.info("Loading model from %s", MODEL_PATH)
    artifact = joblib.load(MODEL_PATH)
    STATE.model = artifact["model"]
    STATE.feature_cols = artifact["feature_cols"]
    log.info("Loaded model with %d features", len(STATE.feature_cols))

    log.info("Loading baselines from %s", BASELINES_PATH)
    baselines = json.loads(BASELINES_PATH.read_text())

    # Detect whether the currently-loaded model was trained on TWAP-60
    # features. The trainer stamps this into training_metadata.json.
    metadata_path = REPO_ROOT / "src" / "model" / "training_metadata.json"
    use_twap = False
    if metadata_path.exists():
        try:
            md = json.loads(metadata_path.read_text())
            use_twap = bool(md.get("trained_on_twap", False))
        except (json.JSONDecodeError, OSError) as e:
            log.warning("could not read trained_on_twap from metadata: %s", e)

    STATE.engine = LiveFeatureEngine(baselines, use_twap_for_model=use_twap)
    log.info("Engine ready (avg_15min_volume=%.2f, avg_trade_rate=%.2f/s, "
             "use_twap_for_model=%s)",
             baselines["avg_15min_volume"], baselines["avg_trade_rate_per_sec"],
             use_twap)

    # Backfill the engine with the last 30 min of 1-min candles so the
    # current-window open price is the TRUE open even if we started mid-window.
    # Run in a thread so the synchronous urllib call doesn't block the loop.
    try:
        await asyncio.to_thread(backfill_engine, STATE.engine, "BTC-USD")
    except Exception as e:  # noqa: BLE001
        log.warning("Backfill threw: %s — proceeding without it", e)

    # Open the persistent prediction log (append-only, survives restart)
    try:
        PREDICTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE.log_fh = open(PREDICTION_LOG_PATH, "a", encoding="utf-8")
        log.info("Logging predictions to %s", PREDICTION_LOG_PATH)
    except OSError as e:
        log.warning("Could not open prediction log %s: %s", PREDICTION_LOG_PATH, e)
        STATE.log_fh = None

    # Replay the persistent log to rehydrate the auto-tuner so a restart
    # doesn't reset arb_thresholds to defaults for the next 10 windows.
    rehydrate_autotune_state()
    # Also rehydrate the current window's per-second chart history so a
    # mid-window restart paints the chart back to sec 0 instead of from
    # the restart moment.
    rehydrate_current_window_history()

    # Live Kalshi order placement client — only created when LIVE_TRADING=1.
    # Keep a long-lived AsyncClient so we reuse TCP connections (saves ~10ms
    # per order on us-east-1 + Kalshi handshakes).
    if LIVE_TRADING_ENABLED:
        import httpx
        STATE.order_client = httpx.AsyncClient(timeout=10)
        log.info("[startup] LIVE_TRADING=1 — Kalshi order client initialized")
    else:
        log.info("[startup] LIVE_TRADING not set — paper mode only")

    consumer_task = asyncio.create_task(coinbase_consumer())
    kalshi_task = asyncio.create_task(kalshi_consumer(STATE.kalshi))
    publisher_task = asyncio.create_task(prediction_publisher())
    try:
        yield
    finally:
        for t in (consumer_task, kalshi_task, publisher_task):
            t.cancel()
        for t in (consumer_task, kalshi_task, publisher_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if STATE.order_client is not None:
            try:
                await STATE.order_client.aclose()
            except Exception:
                pass
        if STATE.log_fh is not None:
            try:
                STATE.log_fh.close()
            except Exception:
                pass


app = FastAPI(lifespan=lifespan, title="BTC 15-min predictor")

# Local-only server — permissive CORS so a file:// page can fetch /model_info
# and any future REST endpoints. WebSocket already works cross-origin without
# this middleware.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def rehydrate_current_window_history() -> None:
    """Reload the current 15-min window's per-second snapshots from the
    persistent log into STATE.history, so a new browser tab paints the
    YES-probability chart from sec 0 (not from the restart moment).
    """
    import time as _t
    from collections import defaultdict
    if not PREDICTION_LOG_PATH.exists():
        return
    # The current window-open ts in epoch seconds (15-min boundary).
    now = _t.time()
    cur_wts = (int(now) // 900) * 900
    rows: list[dict] = []
    try:
        with open(PREDICTION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("window_open_ts") == cur_wts:
                    rows.append(r)
    except OSError as e:
        log.warning("[rehydrate-history] log read failed: %s", e)
        return
    if not rows:
        return
    rows.sort(key=lambda r: r.get("sample_sec", 0))
    seen_secs = set()
    slim: list[dict] = []
    for r in rows:
        sec = r.get("sample_sec")
        if sec is None or sec in seen_secs:
            continue
        seen_secs.add(sec)
        ymid = r.get("kalshi_yes_mid")
        slim.append({
            "sample_sec":     sec,
            "p_green":        r.get("p_green"),
            "window_open_ts": cur_wts,
            "kalshi":         {"yes_mid": ymid} if ymid is not None else None,
        })
    STATE.history = slim
    STATE.history_window_ts = cur_wts
    log.info("[rehydrate-history] Restored %d chart points for current window "
             "(sec %d → %d)",
             len(slim),
             slim[0]["sample_sec"] if slim else -1,
             slim[-1]["sample_sec"] if slim else -1)

    # Also replay the arb tracker on those same rows so the in-flight trade
    # list survives a mid-window restart. We use the CURRENT (just-seeded)
    # thresholds — same params the auto-tuner will use for the rest of the
    # window.
    th = dict(STATE.arb_thresholds)
    STATE.arb_tracker.current_window_ts = cur_wts
    for r in rows:
        sec = r.get("sample_sec")
        if sec is None: continue
        pred = {
            "sample_sec":     sec,
            "window_open_ts": cur_wts,
            "p_green":        r.get("p_green"),
            "current_price":  r.get("current_price"),
            "window_open_px": r.get("window_open_px"),
            "kalshi":         {
                "yes_bid": r.get("kalshi_yes_bid"),
                "yes_ask": r.get("kalshi_yes_ask"),
                "yes_mid": r.get("kalshi_yes_mid"),
            },
        }
        arb = compute_arb_signal(pred, pred["kalshi"], th)
        STATE.arb_tracker.process_tick(pred, arb)
    aw = STATE.arb_tracker.as_payload()
    log.info("[rehydrate-history] Replayed arb tracker for current window: "
             "%d trades, $%+.3f live P&L",
             aw["n"], aw["total_pnl"])


def rehydrate_autotune_state() -> None:
    """Restore STATE.window_pnl_log and STATE.arb_thresholds from disk.

    For each historical window we want the row to use the thresholds the
    auto-tuner WOULD have had active at that moment. There are two sources:
      • window_pnl.jsonl — recorded the REAL thresholds at live settle time.
        Authoritative for any window that's in the file.
      • For windows NOT in the file (anything pre-dating the new ledger),
        walk-forward reconstruct: window i uses the optimum of the prior
        AUTOTUNE_WINDOW_LOOKBACK windows. Tuner is a deterministic function
        of prior windows, so the reconstruction is exact modulo manual
        overrides we never make.

    Forward-going `arb_thresholds` is seeded from the same walk-forward
    output (= the optimum derived from the most recent LOOKBACK windows).
    """
    LB = STATE.AUTOTUNE_WINDOW_LOOKBACK
    MIN_N = STATE.AUTOTUNE_AFTER_N_WINDOWS

    # Load the live ledger (may be empty/missing). Index by window_open_ts.
    live_rows: dict[float, dict] = {}
    if WINDOW_PNL_LOG_PATH.exists():
        try:
            with open(WINDOW_PNL_LOG_PATH, encoding="utf-8") as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    wts = e.get("window_open_ts")
                    if wts is not None:
                        live_rows[wts] = e   # last write wins (re-settlement)
        except OSError as e:
            log.warning("[rehydrate] %s read failed: %s",
                        WINDOW_PNL_LOG_PATH.name, e)

    valid = _load_valid_completed_windows()
    if not valid and not live_rows:
        log.info("[rehydrate] no historical data yet — starting with defaults")
        return

    # Walk-forward across `valid` (predictions.jsonl) computing per-window
    # reconstructed thresholds. Any window missing from live_rows uses the
    # reconstructed entry; live rows take precedence.
    #
    # Optimization: th_at_i is only USED for rows not in live_rows (live
    # rows are taken verbatim). For installs with ledger fully covering
    # the prediction-log window range, this skips ~all of the expensive
    # 2401-combo grid searches and cuts rehydrate from minutes back to
    # seconds. The grid still runs at the END to seed forward thresholds.
    last_th = dict(DEFAULT_ARB_THRESHOLDS)
    merged_entries: list[dict] = []
    for i, (w, pts_sorted, opx_, cpx_, yw) in enumerate(valid):
        if w in live_rows:
            entry = dict(live_rows[w])      # use the live record verbatim
            entry["rehydrated"] = False     # live = LIVE chip
            merged_entries.append(entry)
            continue                        # skip threshold reconstruction
        # ── replay row — needs reconstructed thresholds ────────────────
        if i >= MIN_N:
            prior = valid[max(0, i - LB):i]
            window_data = [(p, ywp) for _, p, _o, _c, ywp in prior]
            th_at_i = find_optimal_thresholds(window_data) or dict(last_th)
        else:
            th_at_i = dict(DEFAULT_ARB_THRESHOLDS)
        last_th = th_at_i
        br = simulate_window_breakdown(
            pts_sorted, yw,
            th_at_i["k_yes"], th_at_i["k_no"],
            th_at_i["d_yes"], th_at_i["d_no"],
        )
        entry = {
            "window_open_ts":     w,
            "settled_at_ts":      w + 900.0,
            "outcome":            "UP" if yw else "DOWN",
            "delta_bps":          round((cpx_/opx_ - 1) * 10000, 2),
            "open_px":            opx_, "close_px": cpx_,
            "thresholds":         th_at_i,
            "trades":             br["n"],
            "wins":               br["wins"],
            "losses":             br["losses"],
            "pnl":                round(br["pnl"], 4),
            "winner_avg_entry":   round(br["winner_avg_entry"], 4) if br["winner_avg_entry"] is not None else None,
            "loser_avg_entry":    round(br["loser_avg_entry"],  4) if br["loser_avg_entry"]  is not None else None,
            "yes_n":              br["yes_n"],
            "no_n":               br["no_n"],
            "rehydrated":         True,
        }
        merged_entries.append(entry)

    # Include any live rows whose window isn't in `valid` (data gap in
    # predictions.jsonl can leave a settled row without a clean window).
    valid_ws = {w for w, *_ in valid}
    for w, e in live_rows.items():
        if w not in valid_ws:
            merged_entries.append({**e, "rehydrated": False})
    merged_entries.sort(key=lambda e: e.get("window_open_ts", 0))
    STATE.window_pnl_log = merged_entries[-REHYDRATE_HISTORY_DEPTH:]

    # Forward-going thresholds: same walk-forward optimum on the most
    # recent LOOKBACK windows.
    if len(valid) >= MIN_N:
        prior = valid[-LB:]
        window_data = [(p, ywp) for _, p, _o, _c, ywp in prior]
        new_th = find_optimal_thresholds(window_data)
        if new_th:
            STATE.arb_thresholds = new_th

    th = STATE.arb_thresholds
    n_live = sum(1 for e in STATE.window_pnl_log if not e.get("rehydrated"))
    n_repl = len(STATE.window_pnl_log) - n_live
    log.info(
        "[rehydrate] Restored %d settled-window rows (%d LIVE from %s, "
        "%d REPLAY walk-forward reconstructed); forward thresholds "
        "k_yes=%.2f k_no=%.2f d_yes=%.2f d_no=%.2f",
        len(STATE.window_pnl_log), n_live, WINDOW_PNL_LOG_PATH.name, n_repl,
        th["k_yes"], th["k_no"], th["d_yes"], th["d_no"],
    )


def _load_valid_completed_windows() -> list[tuple]:
    """Read predictions.jsonl, return [(window_open_ts, pts_sorted, opx, cpx, yes_won)]
    for every completed (sec>=870) window with clean (non-NaN) prices.

    `yes_won` uses Kalshi's late-window YES_mid as ground truth (not TWAP),
    so the simulator's W/L matches what Kalshi actually paid out.
    """
    import math
    from collections import defaultdict
    if not PREDICTION_LOG_PATH.exists():
        return []
    rows: list[dict] = []
    try:
        with open(PREDICTION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try: rows.append(json.loads(line))
                except json.JSONDecodeError: continue
    except OSError:
        return []
    by_win: dict[float, list[dict]] = defaultdict(list)
    for r in rows:
        wts = r.get("window_open_ts")
        if wts is not None:
            by_win[wts].append(r)
    valid = []
    for w, pts in by_win.items():
        if max(p.get("sample_sec", 0) for p in pts) < 870: continue
        pts_sorted = sorted(pts, key=lambda p: p.get("sample_sec", 0))
        opx_ = pts_sorted[0].get("window_open_px")
        cpx_ = pts_sorted[-1].get("current_price")
        if opx_ is None or cpx_ is None: continue
        if not (math.isfinite(opx_) and math.isfinite(cpx_)): continue
        last = pts_sorted[-1]
        yes_won = _determine_outcome(
            opx_, cpx_, last.get("kalshi_yes_mid"), last.get("sample_sec"),
        )
        valid.append((w, pts_sorted, opx_, cpx_, yes_won))
    valid.sort(key=lambda x: x[0])
    return valid


async def on_window_settled(window_ts: float):
    """Called once when a 15-min window has just rolled.

    1. Reads the window's rows from the persistent log
    2. Determines the outcome (TWAP close vs open)
    3. Simulates the window with the thresholds that were ACTIVE during it
    4. Logs the P&L and pushes to STATE.window_pnl_log for the dashboard
    5. If ≥ AUTOTUNE_AFTER_N_WINDOWS completed windows exist, recomputes
       optimal thresholds from the last AUTOTUNE_WINDOW_LOOKBACK windows
       and updates STATE.arb_thresholds in place
    """
    import time as _t
    from collections import defaultdict
    if not PREDICTION_LOG_PATH.exists():
        return
    # Load the persistent log (cheap — JSONL line scan)
    rows: list[dict] = []
    try:
        with open(PREDICTION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try: rows.append(json.loads(line))
                except json.JSONDecodeError: continue
    except OSError as e:
        log.warning("on_window_settled: log read failed: %s", e)
        return
    by_win: dict[float, list[dict]] = defaultdict(list)
    for r in rows:
        wts = r.get("window_open_ts")
        if wts is not None:
            by_win[wts].append(r)
    win_rows = by_win.get(window_ts) or []
    if not win_rows:
        return
    import math
    win_rows.sort(key=lambda r: r.get("sample_sec", 0))
    first = win_rows[0]; last = win_rows[-1]
    opx = first.get("window_open_px")
    cpx = last.get("current_price")
    if opx is None or cpx is None: return
    if not (math.isfinite(opx) and math.isfinite(cpx)):
        log.warning("[WINDOW %s] skipping settlement — NaN open/close px (data gap)",
                    _t.strftime("%H:%M", _t.gmtime(window_ts)))
        return
    # Ground truth = Kalshi's market verdict at settle, not our TWAP comparison.
    # See _determine_outcome doc for rationale.
    last_ymid = last.get("kalshi_yes_mid")
    last_sec  = last.get("sample_sec")
    yes_won = _determine_outcome(opx, cpx, last_ymid, last_sec)
    if last_ymid is not None and last_sec is not None and last_sec >= 870:
        twap_says_up = cpx > opx
        if twap_says_up != yes_won:
            log.warning(
                "[WINDOW %s] outcome flip: TWAP says %s (cpx-opx=%+.2f) but "
                "Kalshi YES_mid=%.3f at sec %d → Kalshi wins, using %s",
                _t.strftime("%H:%M", _t.gmtime(window_ts)),
                "UP" if twap_says_up else "DOWN",
                cpx - opx, last_ymid, last_sec,
                "UP" if yes_won else "DOWN",
            )
    delta_bps = (cpx / opx - 1.0) * 10000.0

    th = dict(STATE.arb_thresholds)
    br = simulate_window_breakdown(
        win_rows, yes_won,
        th["k_yes"], th["k_no"], th["d_yes"], th["d_no"],
    )
    pnl, n, won, lost = br["pnl"], br["n"], br["wins"], br["losses"]
    entry = {
        "window_open_ts":     window_ts,
        "settled_at_ts":      _t.time(),
        "outcome":            "UP" if yes_won else "DOWN",
        "delta_bps":          round(delta_bps, 2),
        "open_px":            opx,
        "close_px":           cpx,
        "thresholds":         th,
        "trades":             n,
        "wins":               won,
        "losses":             lost,
        "pnl":                round(pnl, 4),
        # Per-outcome avg entry prices so the dashboard can show
        # "wins × avg_winner_entry / losses × avg_loser_entry".
        "winner_avg_entry":   round(br["winner_avg_entry"], 4) if br["winner_avg_entry"] is not None else None,
        "loser_avg_entry":    round(br["loser_avg_entry"],  4) if br["loser_avg_entry"]  is not None else None,
        "yes_n":              br["yes_n"],
        "no_n":               br["no_n"],
    }
    STATE.window_pnl_log.append(entry)
    if len(STATE.window_pnl_log) > 100:
        STATE.window_pnl_log = STATE.window_pnl_log[-100:]
    # Persist to disk — survives restart, source of truth for the auto-tuner
    # and the dashboard's settlement-history table.
    _append_jsonl(WINDOW_PNL_LOG_PATH, entry)
    log.info(
        "[WINDOW %s settled] %s (%+.2f bps) · thresholds k_yes=%.2f k_no=%.2f d_yes=%.2f d_no=%.2f · %d trades = %dW/%dL · P&L %s$%.3f",
        _t.strftime("%H:%M", _t.gmtime(window_ts)),
        entry["outcome"], delta_bps,
        th["k_yes"], th["k_no"], th["d_yes"], th["d_no"],
        n, won, lost, "+" if pnl >= 0 else "-", abs(pnl),
    )

    # Auto-tune from the last N completed windows once we have enough.
    # Filter for clean (non-NaN, non-None) prices so data gaps don't poison
    # the optimum.
    valid = []
    for w, rs in by_win.items():
        if max(p.get("sample_sec", 0) for p in rs) < 870: continue
        pts_sorted = sorted(rs, key=lambda p: p.get("sample_sec", 0))
        opx_ = pts_sorted[0].get("window_open_px")
        cpx_ = pts_sorted[-1].get("current_price")
        if opx_ is None or cpx_ is None: continue
        if not (math.isfinite(opx_) and math.isfinite(cpx_)): continue
        last = pts_sorted[-1]
        yw_ = _determine_outcome(opx_, cpx_,
                                 last.get("kalshi_yes_mid"), last.get("sample_sec"))
        valid.append((w, pts_sorted, opx_, cpx_, yw_))
    valid.sort(key=lambda x: x[0])
    completed = valid  # keep variable name for the log line below
    if len(valid) >= STATE.AUTOTUNE_AFTER_N_WINDOWS:
        lookback = valid[-STATE.AUTOTUNE_WINDOW_LOOKBACK:]
        window_data = [(pts_sorted, ywp)
                       for _, pts_sorted, _o, _c, ywp in lookback]
        new_th = find_optimal_thresholds(window_data)
        if new_th and new_th != STATE.arb_thresholds:
            old = dict(STATE.arb_thresholds)
            STATE.arb_thresholds = new_th
            log.info(
                "[AUTOTUNE] After %d completed windows, retuning from last %d: "
                "k_yes %.2f→%.2f · k_no %.2f→%.2f · d_yes %.2f→%.2f · d_no %.2f→%.2f",
                len(completed), len(lookback),
                old["k_yes"], new_th["k_yes"], old["k_no"], new_th["k_no"],
                old["d_yes"], new_th["d_yes"], old["d_no"], new_th["d_no"],
            )


@app.get("/window_pnl_log")
async def window_pnl_log_endpoint(limit: int = 20):
    """Per-window settlement log: list of completed-window P&L summaries with
    the thresholds that were ACTIVE during each window."""
    return {
        "entries": STATE.window_pnl_log[-max(1, limit):],
        "active_thresholds": dict(STATE.arb_thresholds),
    }


@app.get("/arb_window")
async def arb_window_endpoint():
    """Authoritative list of arb trades for the CURRENT 15-min window with
    live mark-to-market P&L. Dashboard is a read-only renderer of this."""
    return STATE.arb_tracker.as_payload()


@app.get("/arb_trades")
async def arb_trades_endpoint(limit: int = 200):
    """Historical settled-arb-trade ledger from disk. Each line of
    arb_trades.jsonl is one trade with its final outcome and P&L.
    """
    if not ARB_TRADES_LOG_PATH.exists():
        return {"entries": [], "log_path": str(ARB_TRADES_LOG_PATH)}
    rows: list[dict] = []
    try:
        with open(ARB_TRADES_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try: rows.append(json.loads(line))
                except json.JSONDecodeError: continue
    except OSError as e:
        return {"error": str(e), "log_path": str(ARB_TRADES_LOG_PATH)}
    return {"entries": rows[-max(1, limit):],
            "total_in_log": len(rows),
            "log_path": str(ARB_TRADES_LOG_PATH)}


@app.get("/optimal_settings")
async def optimal_settings(n_windows: int = 1):
    """Parameter sweep over the last N completed 15-min windows.

    Returns the (K_yes, K_no, Δ_yes, Δ_no) combination that would have
    maximised hold-to-close P&L per share, together with how the current
    live setting (0.45 / 0.45 / 0.12 / 0.12) ranks against it.
    """
    from collections import defaultdict
    if not PREDICTION_LOG_PATH.exists():
        return {"error": "no prediction log yet"}
    rows = []
    try:
        with open(PREDICTION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError as e:
        return {"error": str(e)}
    if not rows:
        return {"error": "log is empty"}

    by_win: dict[float, list[dict]] = defaultdict(list)
    for r in rows:
        wts = r.get("window_open_ts")
        if wts is not None:
            by_win[wts].append(r)

    # Completed = has a sample at or past sec 870 AND clean open/close prices
    # (filters out interrupted recordings with NaN px).
    import math
    completed = []
    for w, pts in by_win.items():
        if not pts or max(p.get("sample_sec", 0) for p in pts) < 870: continue
        pts_s = sorted(pts, key=lambda p: p.get("sample_sec", 0))
        opx_ = pts_s[0].get("window_open_px"); cpx_ = pts_s[-1].get("current_price")
        if opx_ is None or cpx_ is None: continue
        if not (math.isfinite(opx_) and math.isfinite(cpx_)): continue
        completed.append(w)
    completed.sort()
    if not completed:
        return {"error": "no completed windows yet — need at least one window past sec 870"}

    selected = completed[-max(1, n_windows):]
    win_pts:    dict[float, list[dict]] = {}
    win_info:   list[dict] = []
    win_yeswon: dict[float, bool] = {}
    for w in selected:
        pts = sorted(by_win[w], key=lambda p: p.get("sample_sec", 0))
        win_pts[w] = pts
        first, last = pts[0], pts[-1]
        opx = first.get("window_open_px")
        cpx = last.get("current_price")
        yes_won = _determine_outcome(opx, cpx,
                                     last.get("kalshi_yes_mid"), last.get("sample_sec"))
        win_yeswon[w] = yes_won
        win_info.append({
            "window_open_ts": w,
            "outcome":        "UP" if yes_won else "DOWN",
            "open_px":        opx,
            "close_px":       cpx,
            "delta_bps":      round((cpx / opx - 1) * 10000, 2) if (opx and cpx) else None,
            "n_rows":         len(pts),
        })

    # Reuse the shared simulate_window from kalshi_feed for consistency
    from src.collector.kalshi_feed import K_GRID, D_GRID
    results: list[tuple[float, int, float, float, float, float]] = []
    for k_yes in K_GRID:
        for k_no in K_GRID:
            for d_yes in D_GRID:
                for d_no in D_GRID:
                    total_pnl = 0.0; total_n = 0
                    for w in selected:
                        pnl, nn, _, _ = simulate_window(
                            win_pts[w], win_yeswon[w],
                            k_yes, k_no, d_yes, d_no,
                        )
                        total_pnl += pnl; total_n += nn
                    # Round P&L so float jitter doesn't fragment ties.
                    results.append((round(total_pnl, 6), total_n,
                                    k_yes, k_no, d_yes, d_no))
    # Sort: highest P&L first; on tie prefer the *most conservative* (highest
    # K and Δ) combo — that's the tightest filter still extracting this P&L,
    # which is the least overfit choice when many combos tie on thin data.
    results.sort(key=lambda r: (-r[0], -r[2], -r[3], -r[4], -r[5]))

    top_pnl, top_n, ky, kn, dy, dn = results[0]
    n_tied = sum(1 for r in results if r[0] == top_pnl)
    CUR_KEY = (0.45, 0.45, 0.12, 0.12)
    cur = next((r for r in results if (r[2], r[3], r[4], r[5]) == CUR_KEY), None)
    cur_rank = next((i for i, r in enumerate(results)
                     if (r[2], r[3], r[4], r[5]) == CUR_KEY), -1) + 1

    return {
        "n_windows_analyzed": len(selected),
        "n_windows_requested": n_windows,
        "total_combos": len(results),
        "windows": win_info,
        "top": {
            "k_yes": ky, "k_no": kn, "d_yes": dy, "d_no": dn,
            "pnl": round(top_pnl, 3), "trades": top_n,
            "n_tied": n_tied,
        },
        "current": {
            "k_yes": CUR_KEY[0], "k_no": CUR_KEY[1],
            "d_yes": CUR_KEY[2], "d_no": CUR_KEY[3],
            "pnl":    round(cur[0], 3) if cur else None,
            "trades": cur[1]            if cur else None,
            "rank":   cur_rank          if cur_rank > 0 else None,
        },
        "gain_if_optimal": round(top_pnl - (cur[0] if cur else 0), 3),
    }


@app.get("/prediction_history")
async def prediction_history(hours: float = 6):
    """Recent slice of the persistent prediction log, for trend analysis.

    Returns rows as dicts; client can chart model P vs Kalshi mid over
    hours / days. The log is JSONL on disk — easy to grep / pandas-read.
    """
    if not PREDICTION_LOG_PATH.exists():
        return {"rows": [], "log_path": str(PREDICTION_LOG_PATH)}
    import time as _t
    cutoff = _t.time() - max(0.0, hours) * 3600.0
    rows: list[dict] = []
    # Tail-scan: read the file from the end and stop when we hit the cutoff.
    # Simple line-by-line scan is fine for ~17 MB/day worth of log.
    try:
        with open(PREDICTION_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("wall_ts", 0) >= cutoff:
                    rows.append(r)
    except OSError as e:
        return {"error": str(e), "rows": []}
    return {"rows": rows, "log_path": str(PREDICTION_LOG_PATH)}


@app.get("/btc_candles")
async def btc_candles(hours: int = 2):
    """Proxy 1-min OHLC candles from Coinbase Advanced Trade REST.

    Returns the candles list as-is so the browser doesn't have to deal with
    CORS or rate-limit headers itself.
    """
    import time as _time
    import urllib.error
    import urllib.request

    end = int(_time.time())
    start = end - max(1, hours) * 3600
    url = (f"https://api.coinbase.com/api/v3/brokerage/market/products/"
           f"BTC-USD/candles?start={start}&end={end}&granularity=ONE_MINUTE")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        candles = data.get("candles") or []
        # Sort ascending by start; values come as strings, cast for JS
        out = []
        for c in candles:
            try:
                out.append({
                    "t":      int(c["start"]) * 1000,        # ms epoch
                    "open":   float(c["open"]),
                    "high":   float(c["high"]),
                    "low":    float(c["low"]),
                    "close":  float(c["close"]),
                    "volume": float(c["volume"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        out.sort(key=lambda r: r["t"])
        return {"candles": out}
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
        return {"error": str(e), "candles": []}


@app.get("/model_info")
async def model_info():
    """Return a compact summary of the currently-loaded model's training metadata."""
    md_path = REPO_ROOT / "src" / "model" / "training_metadata.json"
    if not md_path.exists():
        return {"error": "no training_metadata.json on disk"}
    try:
        md = json.loads(md_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return {"error": str(e)}
    metrics = md.get("metrics", {}) or {}
    return {
        "trained_on_twap": md.get("trained_on_twap", False),
        "rows":            md.get("rows"),
        "windows":         md.get("windows"),
        "date_range":      md.get("date_range"),
        "best_iteration":  md.get("best_iteration"),
        "half_life_days":  md.get("half_life_days"),
        "n_features":      md.get("n_features"),
        "split":           md.get("split"),
        "metrics": {
            "accuracy": metrics.get("accuracy"),
            "auc":      metrics.get("auc"),
            "brier":    metrics.get("brier"),
        },
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "model_loaded": STATE.model is not None,
        "engine_ready": STATE.engine is not None and STATE.engine.current_window_ns is not None,
        "kalshi": STATE.kalshi.to_dict(),
        "subscribers": len(STATE.subscribers),
        "last_trade_ts_ns": STATE.last_trade_ts_ns,
        "latest_prediction": STATE.latest_prediction,
    }


@app.get("/")
async def root():
    return FileResponse(HTML_PATH)


@app.websocket("/ws/predictions")
async def predictions_ws(ws: WebSocket):
    await ws.accept()
    STATE.subscribers.add(ws)
    log.info("Client connected — %d subscribers", len(STATE.subscribers))
    try:
        # Hydrate the new tab in one round-trip so EVERY panel is correct
        # immediately (instead of waiting up to 200ms for the first live
        # tick). Two messages, in this order:
        #   1. latest_prediction — full payload: kalshi, arb thresholds,
        #      arb_window (server-side trade tracker with live P&L), candle
        #      meta, features, SHAP, etc. Populates every panel.
        #   2. history — slim per-second snapshot of the current window so
        #      the chart paints back to sec 0 instead of starting blank.
        if STATE.latest_prediction is not None:
            await ws.send_json(STATE.latest_prediction)
        if STATE.history:
            await ws.send_json({"type": "history", "points": STATE.history})
        while True:
            # We don't expect client messages, but receive_text() keeps the
            # coroutine alive and surfaces disconnects.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.debug("ws error: %s", e)
    finally:
        STATE.subscribers.discard(ws)
        log.info("Client disconnected — %d subscribers", len(STATE.subscribers))
