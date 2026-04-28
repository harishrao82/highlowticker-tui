"""coinbase_tape_recorder.py — log every Coinbase match (taker fill) to JSONL.

Subscribes to Coinbase Advanced Trade WS market_trades channel for BTC/ETH/SOL/XRP
and writes every trade to ~/.coinbase_tape.jsonl with minimal fields.

Used for offline Twin signal reverse-engineering: for each Twin first-5m-trade
event, examine the taker tape in the prior 30/60/120s to find features that
predict it.
"""
import asyncio
import json
import time
from pathlib import Path

import websockets

CB_WS = "wss://advanced-trade-ws.coinbase.com"
CB_PRODS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}
LOG_PATH = Path.home() / ".coinbase_tape.jsonl"


async def cb_feed():
    products = list(CB_PRODS.values())
    coin_lookup = {p: c for c, p in CB_PRODS.items()}
    sub = {"type": "subscribe", "product_ids": products, "channel": "market_trades"}

    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                print(f"  WS connected — {products}", flush=True)
                count = 0
                last_print = time.time()
                with open(LOG_PATH, "a") as f:
                    async for raw in ws:
                        try:
                            m = json.loads(raw)
                        except Exception:
                            continue
                        if m.get("channel") != "market_trades":
                            continue
                        recv_ts = time.time()
                        for ev in m.get("events", []):
                            for tr in ev.get("trades", []):
                                pid = tr.get("product_id")
                                coin = coin_lookup.get(pid)
                                if not coin:
                                    continue
                                size = tr.get("size")
                                price = tr.get("price")
                                side_raw = tr.get("side", "").upper()
                                if not size or not price:
                                    continue
                                # 'BUY' = taker bought (lifted ask)
                                # 'SELL' = taker sold (hit bid)
                                taker_side = "buy" if side_raw == "BUY" else "sell"
                                entry = {
                                    "ts": round(recv_ts, 3),
                                    "coin": coin,
                                    "side": taker_side,
                                    "price": float(price),
                                    "size": float(size),
                                    "trade_id": tr.get("trade_id"),
                                    "exchange_time": tr.get("time"),
                                }
                                f.write(json.dumps(entry) + "\n")
                                count += 1
                        # Periodic flush + status
                        if recv_ts - last_print > 30:
                            f.flush()
                            mb = LOG_PATH.stat().st_size / (1024*1024)
                            print(f"  +{count} trades in last 30s, file: {mb:.1f} MB",
                                  flush=True)
                            count = 0
                            last_print = recv_ts
        except Exception as e:
            print(f"  [reconnect: {e}]", flush=True)
            await asyncio.sleep(2)


async def main():
    print(f"Coinbase tape recorder starting...")
    print(f"  Output: {LOG_PATH}")
    print(f"  Coins: {list(CB_PRODS)}")
    print()
    await cb_feed()


if __name__ == "__main__":
    asyncio.run(main())
