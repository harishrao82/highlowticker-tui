"""CFB-proxy module — sub-second WS feeds from Coinbase, Kraken, Bitstamp,
combined into a 60-second rolling mean of per-second median bid/ask mids.

Matches Kalshi UI delta within 1-3 basis points consistently.

Usage from trader:
    import cfb_proxy
    asyncio.create_task(cfb_proxy.start(['BTC','ETH','SOL','XRP']))
    # ... later ...
    px = cfb_proxy.current('BTC')   # CFB-proxy price, or None if not yet warm

Per-second values are also appended to ~/.cfb_proxy_log.jsonl for backtesting.
"""
import asyncio
import json
import sys
import time
from collections import deque, defaultdict
from pathlib import Path

import websockets

LOG_PATH = Path.home() / ".cfb_proxy_log.jsonl"

CB_WS = "wss://advanced-trade-ws.coinbase.com"
KR_WS = "wss://ws.kraken.com/v2"
BS_WS = "wss://ws.bitstamp.net"

CB_PRODS = {'BTC':'BTC-USD','ETH':'ETH-USD','SOL':'SOL-USD','XRP':'XRP-USD'}
KR_PAIRS = {'BTC':'BTC/USD','ETH':'ETH/USD','SOL':'SOL/USD','XRP':'XRP/USD'}
BS_PAIRS = {'BTC':'btcusd','ETH':'ethusd','XRP':'xrpusd'}   # bitstamp lacks SOL

DEQUE_LEN = 60   # seconds of history to average

# px[coin][ex] = (bid, ask)
_px: dict = defaultdict(dict)
_windows: dict = {}


def _median(xs):
    p = sorted(x for x in xs if x and x > 0)
    if not p: return None
    n = len(p)
    return p[n // 2] if n % 2 else (p[n // 2 - 1] + p[n // 2]) / 2


def _mid(coin, ex):
    ba = _px[coin].get(ex)
    if not ba: return None
    b, a = ba
    if not b or not a or b <= 0 or a <= 0: return None
    return (b + a) / 2


def current(coin: str) -> float | None:
    """Return current CFB-proxy price (60s rolling mean) or None if not warm yet."""
    w = _windows.get(coin)
    if not w: return None
    return sum(w) / len(w)


def latest_mid(coin: str) -> float | None:
    """Return the latest 1-second median across exchanges (no rolling avg)."""
    return _median([_mid(coin, ex) for ex in ('cb', 'kr', 'bs')])


async def _cb_feed(coins):
    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": [CB_PRODS[c] for c in coins if c in CB_PRODS],
                    "channel": "ticker",
                }))
                async for raw in ws:
                    m = json.loads(raw)
                    for ev in m.get('events', []):
                        for t in ev.get('tickers', []):
                            pid = t.get('product_id')
                            b, a = t.get('best_bid'), t.get('best_ask')
                            if pid and b and a:
                                coin = next((c for c, p in CB_PRODS.items() if p == pid), None)
                                if coin: _px[coin]['cb'] = (float(b), float(a))
        except Exception as e:
            print(f"[cfb_proxy CB reconnect: {e}]", file=sys.stderr)
            await asyncio.sleep(2)


async def _kr_feed(coins):
    pairs = [KR_PAIRS[c] for c in coins if c in KR_PAIRS]
    while True:
        try:
            async with websockets.connect(KR_WS, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "ticker", "symbol": pairs},
                }))
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get('channel') == 'ticker':
                        for d in m.get('data', []):
                            sym = d.get('symbol')
                            b, a = d.get('bid'), d.get('ask')
                            if sym and b and a:
                                coin = next((c for c, p in KR_PAIRS.items() if p == sym), None)
                                if coin: _px[coin]['kr'] = (float(b), float(a))
        except Exception as e:
            print(f"[cfb_proxy KR reconnect: {e}]", file=sys.stderr)
            await asyncio.sleep(2)


async def _bs_feed(coins):
    while True:
        try:
            async with websockets.connect(BS_WS, ping_interval=20) as ws:
                for c in coins:
                    if c in BS_PAIRS:
                        await ws.send(json.dumps({
                            "event": "bts:subscribe",
                            "data": {"channel": f"order_book_{BS_PAIRS[c]}"},
                        }))
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get('event') == 'data':
                        ch = m.get('channel', '')
                        pair = ch.replace('order_book_', '')
                        coin = next((c for c, p in BS_PAIRS.items() if p == pair), None)
                        d = m.get('data', {})
                        bids, asks = d.get('bids', []), d.get('asks', [])
                        if coin and bids and asks:
                            _px[coin]['bs'] = (float(bids[0][0]), float(asks[0][0]))
        except Exception as e:
            print(f"[cfb_proxy BS reconnect: {e}]", file=sys.stderr)
            await asyncio.sleep(2)


async def _sampler(coins):
    """Every second, push the latest median into each coin's rolling deque,
    and append (ts, coin, mid, cfb) to the log file for offline backtesting."""
    for c in coins:
        _windows[c] = deque(maxlen=DEQUE_LEN)
    while True:
        ts = time.time()
        try:
            with open(LOG_PATH, 'a') as f:
                for c in coins:
                    m = latest_mid(c)
                    if m:
                        _windows[c].append(m)
                    cfb = current(c)
                    f.write(json.dumps({
                        'ts':   round(ts, 2),
                        'coin': c,
                        'mid':  round(m, 4) if m else None,
                        'cfb':  round(cfb, 4) if cfb else None,
                    }) + "\n")
        except Exception as e:
            print(f"[cfb_proxy log err: {e}]", file=sys.stderr)
        await asyncio.sleep(1.0)


async def start(coins: list[str]) -> None:
    """Launch all 3 WS feeds + the 1Hz sampler. Run as a task from main."""
    coins = [c.upper() for c in coins if c.upper() in CB_PRODS]
    await asyncio.gather(
        _cb_feed(coins),
        _kr_feed(coins),
        _bs_feed(coins),
        _sampler(coins),
    )
