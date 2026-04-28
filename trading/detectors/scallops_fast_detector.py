"""scallops_fast_detector.py — sub-second Scallops trade detection via PM WS.

Same architecture as twin_fast_detector.py but tracks Idolized-Scallops
(0xe1d6b51521bd4365769199f392f9818661bd907c) on the 15-minute crypto markets
(Scallops's signature pattern, vs Twin who plays 5m).

Replaces 10-30s REST polling lag with ~1-2s end-to-end:
  1. Polymarket CLOB WebSocket pushes every trade in real-time (~500ms)
  2. Query Polygon RPC for the tx receipt (~500-1500ms)
  3. Decode OrderFilled event log; if Scallops maker → log + signal

Output: ~/.scallops_fast_signals.jsonl  (same schema as scallops_live_trades.jsonl
        so the main bot can ingest both interchangeably).
"""
import asyncio
import json
import time
from pathlib import Path

import httpx
import websockets

POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com/events"
RPC_URL = "https://polygon-bor-rpc.publicnode.com"

# OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, ...)
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

SCALLOPS_WALLET = "0xe1d6b51521bd4365769199f392f9818661bd907c"

OUTPUT_LOG = Path.home() / ".scallops_fast_signals.jsonl"

# Permissive — emit ALL Scallops BUYs in 15m markets. Bot applies its own
# filters (price ≥ 0.50) when ingesting the log.
MIN_PRICE = 0.0
WINDOW_LEN = 900   # 15 minutes

# Refresh asset list every 8 min so we always cover the next 15m window before it opens
ASSETS_REFRESH_SEC = 480

COINS = ["btc", "eth", "sol", "xrp"]


# (coin, wst_15m) → True if we've already logged a Scallops signal for this window
_acted: set[tuple[str, int]] = set()


async def fetch_active_markets(client: httpx.AsyncClient) -> dict[str, dict]:
    """Return {asset_id: {coin, wst_15m, slug, outcome}} for current+next 15m windows."""
    markets = {}
    now = int(time.time())
    for coin in COINS:
        for offset in [0, 900, -900]:   # current, next, previous
            wst = (now // 900) * 900 + offset
            slug = f"{coin}-updown-15m-{wst}"
            try:
                r = await client.get(GAMMA_API, params={"slug": slug}, timeout=8)
                if r.status_code != 200 or not r.json():
                    continue
                for ev in r.json():
                    for mk in ev.get("markets", []):
                        tk = mk.get("clobTokenIds")
                        if not tk:
                            continue
                        try:
                            tk = json.loads(tk) if isinstance(tk, str) else tk
                        except Exception:
                            continue
                        outcomes = mk.get("outcomes", '["Up","Down"]')
                        try:
                            outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
                        except Exception:
                            outcomes = ["Up", "Down"]
                        for i, asset_id in enumerate(tk):
                            outcome = outcomes[i] if i < len(outcomes) else f"out{i}"
                            markets[asset_id] = {
                                "coin": coin.upper(),
                                "wst_15m": wst,
                                "slug": slug,
                                "outcome": outcome,
                            }
                        break
            except Exception:
                continue
    return markets


async def is_scallops_trade(client: httpx.AsyncClient, tx_hash: str,
                             max_retries: int = 3) -> tuple[bool, dict | None]:
    """Look up tx receipt; return (is_scallops_maker, receipt_subset)."""
    sc_lower = SCALLOPS_WALLET.lower()
    sc_topic = "0x" + "0" * 24 + sc_lower[2:]   # padded to 32 bytes
    for attempt in range(max_retries):
        try:
            r = await client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
            }, timeout=8)
            if r.status_code != 200:
                await asyncio.sleep(0.5)
                continue
            data = r.json()
            result = data.get("result")
            if result is None:
                # Tx not yet confirmed; retry
                await asyncio.sleep(0.7)
                continue
            for log in result.get("logs", []):
                topics = log.get("topics", [])
                if not topics:
                    continue
                if topics[0].lower() != ORDER_FILLED_TOPIC.lower():
                    continue
                # OrderFilled: topic[2] = maker
                if len(topics) >= 3 and topics[2].lower() == sc_topic:
                    return True, {
                        "tx": tx_hash,
                        "block": int(result.get("blockNumber", "0x0"), 16),
                        "from": result.get("from", ""),
                        "to": result.get("to", ""),
                    }
            return False, {"tx": tx_hash}
        except Exception:
            await asyncio.sleep(0.5)
    return False, None


def _log_signal(entry: dict) -> None:
    try:
        with open(OUTPUT_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"[log err] {e}")


async def handle_trade(rpc_client: httpx.AsyncClient,
                       trade: dict, market_info: dict) -> None:
    """Async-process one trade event from the WS."""
    coin = market_info["coin"]
    wst_15m = market_info["wst_15m"]
    key = (coin, wst_15m)
    if key in _acted:
        return

    try:
        price = float(trade.get("price", 0))
        size = float(trade.get("size", 0))
    except Exception:
        return
    if price < MIN_PRICE:
        return

    # Convert ms timestamp from WS to seconds
    try:
        trade_ts_ms = int(trade.get("timestamp", 0))
        trade_ts = trade_ts_ms / 1000.0
    except Exception:
        trade_ts = time.time()

    elapsed = trade_ts - wst_15m
    if elapsed < 0 or elapsed > WINDOW_LEN:
        return

    tx_hash = trade.get("transaction_hash")
    if not tx_hash:
        return

    t0 = time.time()
    is_sc, _ = await is_scallops_trade(rpc_client, tx_hash)
    rpc_lag_ms = (time.time() - t0) * 1000
    if not is_sc:
        return

    # Mark and log (one per coin/window)
    _acted.add(key)
    detect_ts = time.time()
    side_str = "BUY" if str(trade.get("side", "BUY")).upper() == "BUY" else "SELL"
    entry = {
        "fetch_ts": detect_ts,
        "trade_ts": trade_ts,
        "lag_sec": round(detect_ts - trade_ts, 2),
        "rpc_lookup_ms": round(rpc_lag_ms, 0),
        "bot": "Scallops",
        "slug": market_info["slug"],
        "coin": coin,
        "market_type": "15m",
        "side": side_str,
        "outcome": market_info["outcome"],
        "price": price,
        "size": size,
        "notional": round(price * size, 2),
        "tx": tx_hash,
        "window_start_ts": wst_15m,
        "elapsed_in_window": int(elapsed),
        "source": "fast_ws_detector",
    }
    _log_signal(entry)
    print(f"  ★ SCALLOPS-FAST {coin} {market_info['outcome']}@{price:.2f} "
          f"sz={size:.1f} | lag={detect_ts-trade_ts:.1f}s "
          f"(rpc={rpc_lag_ms:.0f}ms) tx={tx_hash[:14]}",
          flush=True)


async def ws_loop():
    """WS subscription loop with:
    - SCHEDULED reconnect at minute 13:30 of each 15m window (dead time)
    - WATCHDOG reconnect on >45s silence
    - EXCEPTION reconnect on any error
    """
    STALE_SEC = 45
    SAFE_RECONNECT_OFFSET_S = 810   # 13:30 into 15m window
    last_event_ts = time.time()
    last_scheduled_reconnect = 0
    async with httpx.AsyncClient(timeout=10) as rpc_client:
        markets = {}
        ws = None
        while True:
            try:
                now = time.time()

                # Window-aligned scheduled reconnect at 13:30 of each 15m window.
                cur_window_start = (int(now) // 900) * 900
                target = cur_window_start + SAFE_RECONNECT_OFFSET_S
                need_refresh = (target <= now and last_scheduled_reconnect < target) or not markets
                if need_refresh:
                    if last_scheduled_reconnect < target:
                        last_scheduled_reconnect = target
                        print(f"  [scheduled reconnect at T+{int(now-cur_window_start)}s]", flush=True)
                    new_markets = await fetch_active_markets(rpc_client)
                    if new_markets:
                        markets = new_markets
                        print(f"  refreshed: {len(markets)} active assets", flush=True)
                        if ws:
                            try: await ws.close()
                            except Exception: pass
                            ws = None

                if ws is None:
                    ws = await websockets.connect(POLY_WS, ping_interval=20)
                    asset_ids = list(markets.keys())
                    await ws.send(json.dumps({"type": "market", "assets_ids": asset_ids}))
                    print(f"  ws subscribed to {len(asset_ids)} assets", flush=True)
                    last_event_ts = time.time()

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                except asyncio.TimeoutError:
                    if time.time() - last_event_ts > STALE_SEC:
                        print(f"  [stale {int(time.time()-last_event_ts)}s — reconnect]", flush=True)
                        try: await ws.close()
                        except Exception: pass
                        ws = None
                        last_event_ts = time.time()
                    continue

                last_event_ts = time.time()
                m = json.loads(raw)
                items = m if isinstance(m, list) else [m]
                for item in items:
                    if item.get("event_type") != "last_trade_price":
                        continue
                    asset_id = item.get("asset_id")
                    if not asset_id or asset_id not in markets:
                        continue
                    asyncio.create_task(handle_trade(rpc_client, item, markets[asset_id]))
            except Exception as e:
                print(f"  [reconnect: {type(e).__name__} {str(e)[:80]}]", flush=True)
                if ws:
                    try: await ws.close()
                    except Exception: pass
                    ws = None
                await asyncio.sleep(2)


async def main():
    print(f"scallops_fast_detector starting...")
    print(f"  WS: {POLY_WS}")
    print(f"  RPC: {RPC_URL}")
    print(f"  Scallops wallet: {SCALLOPS_WALLET}")
    print(f"  Output: {OUTPUT_LOG}")
    print(f"  Filter: any 15m crypto Scallops BUY (price filter applied downstream)")
    print()
    await ws_loop()


if __name__ == "__main__":
    asyncio.run(main())
