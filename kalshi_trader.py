#!/usr/bin/env python3
"""
kalshi_trader.py — Interactive Kalshi order CLI.

Usage:
    python kalshi_trader.py buy BTC yes 5 0.44                    # buy 5 YES @ 0.44 current window
    python kalshi_trader.py buy BTC yes 5 0.44 --window next      # buy in next window
    python kalshi_trader.py buy BTC yes 5 0.44 --on-fill 0.78     # buy, auto-sell at 0.78 on fill
    python kalshi_trader.py orders                                 # list resting orders
    python kalshi_trader.py orders BTC                             # list resting orders for BTC
    python kalshi_trader.py cancel <order_id>                      # cancel one order
    python kalshi_trader.py cancel-all                             # cancel all resting orders
    python kalshi_trader.py cancel-all BTC                         # cancel all BTC resting orders
    python kalshi_trader.py ticker BTC                             # show current & next ticker
"""
import argparse
import asyncio
import base64
import math
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
ET_OFFSET   = timedelta(hours=-4)

SERIES = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
}

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Auth ─────────────────────────────────────────────────────────────────────

def _headers(method: str, path: str) -> dict:
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
        "Content-Type":            "application/json",
    }


# ── Ticker Resolution ───────────────────────────────────────────────────────

def _ticker_from_close(series: str, close_utc: datetime) -> str:
    close_et = close_utc + ET_OFFSET
    return series + "-" + close_et.strftime("%y%b%d%H%M").upper() + "-" + close_et.strftime("%M")


def _get_window_bounds(which: str) -> tuple[datetime, datetime]:
    now_et    = datetime.now(timezone.utc) + ET_OFFSET
    mins      = (now_et.minute // 15) * 15
    cur_open  = now_et.replace(minute=mins, second=0, microsecond=0)
    cur_close = cur_open + timedelta(minutes=15)
    if which == "next":
        cur_open  += timedelta(minutes=15)
        cur_close += timedelta(minutes=15)
    return cur_open - ET_OFFSET, cur_close - ET_OFFSET


def resolve_ticker(coin: str, window: str = "current") -> str:
    coin = coin.upper()
    series = SERIES[coin]
    open_utc, close_utc = _get_window_bounds(window)
    ticker   = _ticker_from_close(series, close_utc)
    open_et  = open_utc + ET_OFFSET
    close_et = close_utc + ET_OFFSET
    print(f"{coin} {window} window: {ticker}  ({open_et.strftime('%H:%M')}–{close_et.strftime('%H:%M ET')})")
    return ticker


def _cents_to_dollars(p: float | None) -> float | None:
    """Normalize price: if >1 treat as cents (e.g. 92 → 0.92), else keep as dollars."""
    if p is None:
        return None
    return p / 100 if p > 1 else p


# ── Order Helpers ────────────────────────────────────────────────────────────

async def _place_order(client: httpx.AsyncClient, ticker: str, action: str,
                       side: str, qty: int, limit: float) -> dict | None:
    yes_price = f"{limit:.2f}" if side == "yes" else f"{1.0 - limit:.2f}"
    order = {
        "ticker":            ticker,
        "action":            action,
        "side":              side,
        "count":             qty,
        "type":              "limit",
        "yes_price_dollars": yes_price,
        "client_order_id":   str(uuid.uuid4()),
    }
    r = await client.post(
        f"{KALSHI_BASE}/portfolio/orders",
        headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
        content=json.dumps(order),
        timeout=8,
    )
    if r.status_code in (200, 201):
        od = r.json().get("order", {})
        print(f"  {action.upper()} {side.upper()} {qty}ct @ {limit:.2f}  oid={od.get('order_id','')[:8]}")
        return od
    else:
        print(f"  FAILED {r.status_code}: {r.text[:200]}")
        return None


async def _cancel_order(client: httpx.AsyncClient, oid: str) -> bool:
    r = await client.delete(
        f"{KALSHI_BASE}/portfolio/orders/{oid}",
        headers=_headers("DELETE", f"/trade-api/v2/portfolio/orders/{oid}"),
        timeout=8,
    )
    if r.status_code in (200, 204):
        print(f"  Cancelled {oid[:8]}")
        return True
    elif r.status_code == 404:
        print(f"  Already gone {oid[:8]}")
        return True
    else:
        print(f"  Cancel failed {r.status_code}: {r.text[:120]}")
        return False


async def _wait_fill_then_sell(client: httpx.AsyncClient, oid: str, ticker: str,
                               side: str, qty: int, sell_limit: float | None = None,
                               sell_offset: float | None = None,
                               sell_offset_perc: float | None = None):
    """
    Poll until filled, then place a sell.
    sell_limit:       absolute price (e.g. 0.78)
    sell_offset:      fixed offset from fill price (e.g. +0.15 → fill@0.40 sells@0.55)
    sell_offset_perc: percent above fill price (e.g. 10 → fill@0.40 sells@0.44)
    """
    if sell_limit:
        print(f"  Watching {oid[:8]} for fill → will sell @ {sell_limit:.2f}")
    elif sell_offset_perc is not None:
        print(f"  Watching {oid[:8]} for fill → will sell @ fill_price + {sell_offset_perc:.0f}%")
    elif sell_offset is not None:
        print(f"  Watching {oid[:8]} for fill → will sell @ fill_price + {sell_offset:.2f}")
    while True:
        await asyncio.sleep(3)
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/orders/{oid}",
            headers=_headers("GET", f"/trade-api/v2/portfolio/orders/{oid}"),
            timeout=8,
        )
        if r.status_code != 200:
            continue
        order_data = r.json().get("order", {})
        status     = order_data.get("status", "")
        remaining  = int(float(order_data.get("remaining_count_fp", qty)))
        filled     = qty - remaining
        if filled > 0:
            yes_price = float(order_data.get("yes_price_dollars") or
                              order_data.get("yes_price") or 0)
            fill_price = yes_price if side == "yes" else round(1.0 - yes_price, 2)

            if sell_limit:
                price = sell_limit
            elif sell_offset_perc is not None:
                # Round up to next cent
                price = math.ceil(fill_price * (1 + sell_offset_perc / 100) * 100) / 100
            elif sell_offset is not None:
                price = round(fill_price + sell_offset, 2)
            else:
                price = fill_price  # shouldn't happen

            price = min(price, 0.99)
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] FILLED {filled}ct @ {fill_price:.2f} — placing SELL {side.upper()} @ {price:.2f}")
            await _place_order(client, ticker, "sell", side, filled, price)
            return
        if status in ("canceled", "cancelled"):
            print(f"  Order {oid[:8]} was cancelled — no sell placed")
            return


# ── Public Commands ──────────────────────────────────────────────────────────

async def cmd_buy(coin: str, side: str, qty: int, limit: float,
                  window: str = "current", on_fill: float | None = None,
                  on_fill_offset: float | None = None,
                  on_fill_offset_perc: float | None = None):
    ticker = resolve_ticker(coin, window)
    async with httpx.AsyncClient() as client:
        od = await _place_order(client, ticker, "buy", side, qty, limit)
        if od and (on_fill is not None or on_fill_offset is not None or on_fill_offset_perc is not None):
            oid = od.get("order_id", "")
            print(f"  Watching for fill… (Ctrl-C to cancel order and stop)")
            try:
                await _wait_fill_then_sell(client, oid, ticker, side, qty,
                                           sell_limit=on_fill, sell_offset=on_fill_offset,
                                           sell_offset_perc=on_fill_offset_perc)
            except (asyncio.CancelledError, KeyboardInterrupt):
                print(f"\n  Interrupted — cancelling order {oid[:8]}…")
                await _cancel_order(client, oid)
                raise


async def cmd_orders(coin: str | None = None):
    params: dict = {"status": "resting", "limit": 100}
    if coin:
        params["ticker"] = SERIES[coin.upper()]
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("GET", "/trade-api/v2/portfolio/orders"),
            params=params,
            timeout=8,
        )
        if r.status_code == 200:
            orders = r.json().get("orders", [])
            for o in orders:
                side  = o.get("side", "?")
                price = o.get("yes_price_dollars") or o.get("yes_price", "?")
                rem   = o.get("remaining_count_fp", "?")
                print(f"  {o['ticker']}  {o['action']} {side.upper()} {rem}ct @ {price}  status={o['status']}  oid={o['order_id'][:8]}")
            print(f"  ({len(orders)} orders)")
        else:
            print(f"  Failed {r.status_code}: {r.text[:200]}")


async def cmd_cancel(oid: str):
    async with httpx.AsyncClient() as client:
        await _cancel_order(client, oid)


async def cmd_cancel_all(coin: str | None = None):
    params: dict = {"status": "resting", "limit": 100}
    if coin:
        params["ticker"] = SERIES[coin.upper()]
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("GET", "/trade-api/v2/portfolio/orders"),
            params=params,
            timeout=8,
        )
        if r.status_code != 200:
            print(f"  Failed to fetch orders: {r.status_code}")
            return
        orders = r.json().get("orders", [])
        for o in orders:
            await _cancel_order(client, o["order_id"])
        print(f"  Cancelled {len(orders)} orders")


async def cmd_positions(coin: str | None = None):
    """List current positions (open exposure)."""
    async with httpx.AsyncClient() as client:
        params: dict = {"limit": 100, "count_filter": "position", "settlement_status": "unsettled"}
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/positions",
            headers=_headers("GET", "/trade-api/v2/portfolio/positions"),
            params=params,
            timeout=8,
        )
        if r.status_code != 200:
            print(f"  Failed {r.status_code}: {r.text[:200]}")
            return []
        positions = r.json().get("market_positions", [])
        filtered = []
        for p in positions:
            ticker = p.get("ticker", "")
            # Filter by coin if requested
            if coin and coin.upper() not in ticker:
                continue
            yes_qty = int(float(p.get("position", 0)))
            no_qty  = int(float(p.get("total_traded", 0))) - yes_qty  # fallback
            # Kalshi returns position (net yes) and resting orders separately
            # position > 0 means long yes, < 0 means long no
            if yes_qty > 0:
                side, qty = "YES", yes_qty
            elif yes_qty < 0:
                side, qty = "NO", abs(yes_qty)
            else:
                continue
            filtered.append({"ticker": ticker, "side": side.lower(), "qty": qty})
            print(f"  {ticker}  {side} {qty}ct")
        if not filtered:
            print("  No open positions" + (f" for {coin.upper()}" if coin else ""))
        return filtered


async def cmd_sell(coin: str, side: str, qty: int, limit: float | None = None,
                   window: str = "current"):
    """Sell a position. If no limit given, sells at current bid (market sell)."""
    ticker = resolve_ticker(coin, window)
    async with httpx.AsyncClient() as client:
        if limit is None:
            bid, ask = await _fetch_bid_ask(client, ticker, side)
            if bid <= 0:
                print(f"  No bid available for {side.upper()} on {ticker}")
                return
            limit = bid
            print(f"  {side.upper()} market: bid={bid:.2f}  ask={ask:.2f}")
            print(f"  Market selling {qty}ct @ bid {limit:.2f}")
        else:
            print(f"  Limit selling {qty}ct @ {limit:.2f}")
        await _place_order(client, ticker, "sell", side, qty, limit)


async def cmd_dump(coin: str | None = None):
    """Market-sell all open positions (hit the bid)."""
    positions = await cmd_positions(coin)
    if not positions:
        return
    async with httpx.AsyncClient() as client:
        for p in positions:
            ticker, side, qty = p["ticker"], p["side"], p["qty"]
            bid, ask = await _fetch_bid_ask(client, ticker, side)
            if bid <= 0:
                print(f"  No bid for {side.upper()} on {ticker} — skipping")
                continue
            print(f"  Dumping {ticker} {side.upper()} {qty}ct @ bid {bid:.2f}")
            await _place_order(client, ticker, "sell", side, qty, bid)
    print("  Done")


async def _get_market(client: httpx.AsyncClient, ticker: str) -> dict | None:
    """Fetch market snapshot — returns dict with yes_ask_dollars, yes_bid_dollars, etc."""
    r = await client.get(
        f"{KALSHI_BASE}/markets/{ticker}",
        headers=_headers("GET", f"/trade-api/v2/markets/{ticker}"),
        timeout=8,
    )
    if r.status_code == 200:
        return r.json().get("market", {})
    print(f"  Failed to fetch market {ticker}: {r.status_code}")
    return None


async def _fetch_bid_ask(client: httpx.AsyncClient, ticker: str, side: str) -> tuple[float, float]:
    """Fetch current bid/ask for a side. Returns (bid, ask)."""
    mkt = await _get_market(client, ticker)
    if not mkt:
        return 0.0, 0.0
    yes_ask = float(mkt.get("yes_ask_dollars") or 0)
    yes_bid = float(mkt.get("yes_bid_dollars") or 0)
    if side == "yes":
        return yes_bid, yes_ask
    else:
        no_ask = round(1.0 - yes_bid, 2) if yes_bid > 0 else 0.0
        no_bid = round(1.0 - yes_ask, 2) if yes_ask > 0 else 0.0
        return no_bid, no_ask


async def cmd_snipe(coin: str, side: str, qty: int, discount: float = 0.05,
                    level: float | None = None,
                    window: str = "current", on_fill: float | None = None,
                    on_fill_offset: float | None = None,
                    on_fill_offset_perc: float | None = None):
    """Buy at a specific level, or at current ask minus discount (default 5c)."""
    ticker = resolve_ticker(coin, window)
    async with httpx.AsyncClient() as client:
        # For next window, wait until it opens so there's a live book
        if window == "next":
            open_utc, _ = _get_window_bounds("next")
            wait = (open_utc - datetime.now(timezone.utc)).total_seconds()
            if wait > 0:
                open_wall = datetime.now() + timedelta(seconds=wait)
                print(f"  Window opens in {wait:.0f}s ({open_wall.strftime('%H:%M:%S')}) — waiting…")
                await asyncio.sleep(wait)
                # Poll until book is live (up to 30s)
                print(f"  Window open — waiting for book…")
                for _ in range(10):
                    bid, ask = await _fetch_bid_ask(client, ticker, side)
                    if ask > 0 or level is not None:
                        break
                    await asyncio.sleep(3)

        bid, ask = await _fetch_bid_ask(client, ticker, side)
        print(f"  {side.upper()} market: bid={bid:.2f}  ask={ask:.2f}")

        if level is not None:
            limit = level
            print(f"  Placing limit buy @ {limit:.2f}  (explicit level)")
        else:
            if ask <= 0:
                print(f"  No ask available for {side.upper()} on {ticker}")
                return
            limit = round(ask - discount, 2)
            if limit < 0.01:
                limit = 0.01
            print(f"  Placing limit buy @ {limit:.2f}  (ask {ask:.2f} - {discount:.2f})")

        od = await _place_order(client, ticker, "buy", side, qty, limit)
        if od and (on_fill is not None or on_fill_offset is not None or on_fill_offset_perc is not None):
            oid = od.get("order_id", "")
            print(f"  Watching for fill… (Ctrl-C to cancel order and stop)")
            try:
                await _wait_fill_then_sell(client, oid, ticker, side, qty,
                                           sell_limit=on_fill, sell_offset=on_fill_offset,
                                           sell_offset_perc=on_fill_offset_perc)
            except (asyncio.CancelledError, KeyboardInterrupt):
                print(f"\n  Interrupted — cancelling order {oid[:8]}…")
                await _cancel_order(client, oid)
                raise


async def cmd_ticker(coin: str):
    resolve_ticker(coin, "current")
    resolve_ticker(coin, "next")


# ── Price watch helpers ──────────────────────────────────────────────────────

import websockets as _ws_lib

COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_PRODUCT = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}


async def _stream_price(coin: str, price_queue: asyncio.Queue):
    """Stream real-time prices from Coinbase websocket into a queue."""
    product = COINBASE_PRODUCT[coin.upper()]
    sub = {"type": "subscribe", "product_ids": [product], "channel": "ticker"}
    while True:
        try:
            async with _ws_lib.connect(COINBASE_WS, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for evt in msg.get("events", []):
                        for tk in evt.get("tickers", []):
                            p = float(tk.get("price", 0) or 0)
                            if p > 0:
                                # Drain old prices, keep only latest
                                while not price_queue.empty():
                                    try:
                                        price_queue.get_nowait()
                                    except asyncio.QueueEmpty:
                                        break
                                price_queue.put_nowait(p)
        except Exception as e:
            print(f"\n  [WS reconnecting: {e}]")
            await asyncio.sleep(2)


async def cmd_watch(coin: str, action: str, side: str, qty: int,
                    target_price: float, limit: float | None = None,
                    discount: float | None = None,
                    on_fill: float | None = None,
                    on_fill_offset: float | None = None,
                    on_fill_offset_perc: float | None = None):
    """
    Stream Coinbase price in real time.
    When target is hit, place a buy/sell on the CURRENT Kalshi window.
    """
    coin = coin.upper()
    price_queue: asyncio.Queue = asyncio.Queue()

    # Start streaming in background
    stream_task = asyncio.create_task(_stream_price(coin, price_queue))

    # Wait for first price
    print(f"  Connecting to Coinbase stream for {coin}…")
    try:
        spot = await asyncio.wait_for(price_queue.get(), timeout=10)
    except asyncio.TimeoutError:
        print("  Could not get initial price — check connection")
        stream_task.cancel()
        return

    if spot >= target_price:
        direction = "below"
        trigger = lambda p: p <= target_price
    else:
        direction = "above"
        trigger = lambda p: p >= target_price

    print(f"  {coin} spot: ${spot:,.2f}")
    print(f"  Streaming — watching for {coin} to go {direction} ${target_price:,.2f}")
    print(f"  Will {action.upper()} {side.upper()} {qty}ct on current Kalshi window")
    if limit:
        print(f"  Limit: {limit:.2f}")
    elif discount:
        print(f"  Will buy at ask - {discount:.2f}")
    else:
        print(f"  Will use market bid/ask at trigger time")
    if on_fill:
        print(f"  On fill: sell @ {on_fill:.2f}")
    elif on_fill_offset_perc is not None:
        print(f"  On fill: sell @ fill + {on_fill_offset_perc:.0f}%")
    elif on_fill_offset:
        print(f"  On fill: sell @ fill + {on_fill_offset:.2f}")
    print(f"  Ctrl-C to stop\n")

    try:
        async with httpx.AsyncClient() as client:
            price = spot
            while True:
                try:
                    price = await asyncio.wait_for(price_queue.get(), timeout=30)
                except asyncio.TimeoutError:
                    print(f"\n  [no tick in 30s — still connected]", end="")
                    continue

                ts = datetime.now().strftime("%H:%M:%S")
                diff = price - target_price
                print(f"  [{ts}] {coin} ${price:,.2f}  (target ${target_price:,.2f}  diff {diff:+,.2f})    ", end="\r")

                if trigger(price):
                    print(f"\n  [{ts}] TRIGGERED! {coin} ${price:,.2f} went {direction} ${target_price:,.2f}")

                    ticker = resolve_ticker(coin, "current")

                    if action == "buy":
                        bid, ask = await _fetch_bid_ask(client, ticker, side)
                        print(f"  {side.upper()} market: bid={bid:.2f}  ask={ask:.2f}")
                        if limit is not None:
                            buy_price = limit
                        elif discount is not None:
                            if ask <= 0:
                                print(f"  No ask — can't apply discount")
                                return
                            buy_price = round(ask - discount, 2)
                            if buy_price < 0.01:
                                buy_price = 0.01
                            print(f"  Limit buy @ {buy_price:.2f}  (ask {ask:.2f} - {discount:.2f})")
                        else:
                            if ask <= 0:
                                print(f"  No ask — placing at 0.99")
                                ask = 0.99
                            buy_price = ask

                        od = await _place_order(client, ticker, "buy", side, qty, buy_price)
                        if od and (on_fill is not None or on_fill_offset is not None or on_fill_offset_perc is not None):
                            oid = od.get("order_id", "")
                            print(f"  Watching for fill… (Ctrl-C to cancel and stop)")
                            try:
                                await _wait_fill_then_sell(client, oid, ticker, side, qty,
                                                           sell_limit=on_fill, sell_offset=on_fill_offset,
                                                           sell_offset_perc=on_fill_offset_perc)
                            except (asyncio.CancelledError, KeyboardInterrupt):
                                print(f"\n  Interrupted — cancelling order {oid[:8]}…")
                                await _cancel_order(client, oid)
                                raise

                    elif action == "sell":
                        if limit is not None:
                            sell_price = limit
                        else:
                            bid, ask = await _fetch_bid_ask(client, ticker, side)
                            if bid <= 0:
                                print(f"  No bid — placing at 0.01")
                                bid = 0.01
                            sell_price = bid
                            print(f"  {side.upper()} market: bid={bid:.2f}  ask={ask:.2f}")

                        await _place_order(client, ticker, "sell", side, qty, sell_price)

                    print("  Done.")
                    return
    finally:
        stream_task.cancel()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Kalshi manual trader",
        epilog="""\
examples:
  python kalshi_trader.py buy BTC yes 5 0.44                       buy 5 YES @ 0.44 current window
  python kalshi_trader.py buy BTC yes 5 0.44 --window next         buy in next window
  python kalshi_trader.py buy BTC yes 5 0.44 --on-fill 0.78        buy, auto-sell at 0.78 on fill
  python kalshi_trader.py buy BTC yes 5 0.40 --on-fill-offset 0.15 buy, auto-sell at fill+0.15
  python kalshi_trader.py snipe BTC yes 5                           buy at ask - 0.05
  python kalshi_trader.py snipe BTC yes 5 --level 0.35             buy at exact level 0.35 (shows bid/ask)
  python kalshi_trader.py snipe BTC yes 5 --discount 0.03          buy at ask - 0.03
  python kalshi_trader.py snipe BTC yes 5 --on-fill-offset 0.15    snipe + auto-sell at fill+0.15
  python kalshi_trader.py sell BTC yes 50                            market sell 50 YES at bid
  python kalshi_trader.py sell BTC yes 50 --limit 0.70              limit sell 50 YES at 0.70
  python kalshi_trader.py positions                                 list open positions
  python kalshi_trader.py positions BTC                             list BTC positions
  python kalshi_trader.py dump                                      market sell ALL positions
  python kalshi_trader.py dump BTC                                  market sell all BTC positions
  python kalshi_trader.py orders                                    list resting orders
  python kalshi_trader.py orders BTC                                list resting orders for BTC
  python kalshi_trader.py cancel <order_id>                         cancel one order
  python kalshi_trader.py cancel-all                                cancel all resting orders
  python kalshi_trader.py cancel-all BTC                            cancel all BTC resting orders
  python kalshi_trader.py watch BTC buy yes 50 83200                 buy YES when BTC hits $83200
  python kalshi_trader.py watch BTC sell yes 50 84000                sell YES when BTC hits $84000
  python kalshi_trader.py watch BTC buy yes 50 83200 --limit 0.40   buy YES @ 0.40 when BTC hits $83200
  python kalshi_trader.py watch BTC buy yes 50 83200 --discount .03 buy at ask-0.03 when triggered
  python kalshi_trader.py watch BTC buy yes 50 83200 --on-fill-offset-perc 10  sell at fill+10%%
  python kalshi_trader.py ticker BTC                                show current & next ticker
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd")

    p_buy = sub.add_parser("buy", help="Place a limit buy")
    p_buy.add_argument("coin", help="BTC, ETH, SOL, XRP")
    p_buy.add_argument("side", help="yes or no")
    p_buy.add_argument("qty", type=int, help="Number of contracts")
    p_buy.add_argument("limit", type=float, help="Limit price (0.01–0.99)")
    p_buy.add_argument("--window", default="current", choices=["current", "next"])
    p_buy.add_argument("--on-fill", type=float, default=None,
                       help="Auto-sell at this absolute price on fill")
    p_buy.add_argument("--on-fill-offset", type=float, default=None,
                       help="Auto-sell at fill_price + offset (e.g. 0.15)")
    p_buy.add_argument("--on-fill-offset-perc", type=float, default=None,
                       help="Auto-sell at fill_price + N%% (e.g. 10)")

    p_orders = sub.add_parser("orders", help="List resting orders")
    p_orders.add_argument("coin", nargs="?", default=None)

    p_cancel = sub.add_parser("cancel", help="Cancel an order by ID")
    p_cancel.add_argument("oid", help="Order ID")

    p_ca = sub.add_parser("cancel-all", help="Cancel all resting orders")
    p_ca.add_argument("coin", nargs="?", default=None)

    p_snipe = sub.add_parser("snipe", help="Buy at ask minus discount, or at explicit level")
    p_snipe.add_argument("coin", help="BTC, ETH, SOL, XRP")
    p_snipe.add_argument("side", help="yes or no")
    p_snipe.add_argument("qty", type=int, help="Number of contracts")
    p_snipe.add_argument("--level", type=float, default=None,
                         help="Exact limit price (skips ask-discount calc)")
    p_snipe.add_argument("--discount", type=float, default=0.05,
                         help="Cents below ask (default 0.05, ignored if --level set)")
    p_snipe.add_argument("--window", default="current", choices=["current", "next"])
    p_snipe.add_argument("--on-fill", type=float, default=None,
                         help="Auto-sell at this absolute price on fill")
    p_snipe.add_argument("--on-fill-offset", type=float, default=None,
                         help="Auto-sell at fill_price + offset (e.g. 0.15)")
    p_snipe.add_argument("--on-fill-offset-perc", type=float, default=None,
                         help="Auto-sell at fill_price + N%% (e.g. 10)")

    p_pos = sub.add_parser("positions", help="List open positions")
    p_pos.add_argument("coin", nargs="?", default=None)

    p_sell = sub.add_parser("sell", help="Sell a position (market sell if no --limit)")
    p_sell.add_argument("coin", help="BTC, ETH, SOL, XRP")
    p_sell.add_argument("side", help="yes or no")
    p_sell.add_argument("qty", type=int, help="Number of contracts")
    p_sell.add_argument("--limit", type=float, default=None,
                        help="Limit price (omit to sell at bid)")
    p_sell.add_argument("--window", default="current", choices=["current", "next"])

    p_dump = sub.add_parser("dump", help="Market-sell ALL open positions")
    p_dump.add_argument("coin", nargs="?", default=None)

    p_watch = sub.add_parser("watch", help="Watch BTC price, trigger buy/sell at target")
    p_watch.add_argument("coin", help="BTC, ETH, SOL, XRP")
    p_watch.add_argument("action", choices=["buy", "sell"], help="buy or sell")
    p_watch.add_argument("side", help="yes or no")
    p_watch.add_argument("qty", type=int, help="Number of contracts")
    p_watch.add_argument("target", type=float, help="BTC price trigger (e.g. 83200)")
    p_watch.add_argument("--limit", type=float, default=None,
                         help="Kalshi limit price (omit to use market bid/ask)")
    p_watch.add_argument("--discount", type=float, default=None,
                         help="Buy at ask minus discount at trigger time")
    p_watch.add_argument("--on-fill", type=float, default=None,
                         help="Auto-sell at this price on fill")
    p_watch.add_argument("--on-fill-offset", type=float, default=None,
                         help="Auto-sell at fill_price + offset")
    p_watch.add_argument("--on-fill-offset-perc", type=float, default=None,
                         help="Auto-sell at fill_price + N%% (e.g. 10)")

    p_tick = sub.add_parser("ticker", help="Show current & next ticker")
    p_tick.add_argument("coin", help="BTC, ETH, SOL, XRP")

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return

    if args.cmd == "buy":
        asyncio.run(cmd_buy(args.coin, args.side, args.qty,
                            _cents_to_dollars(args.limit),
                            args.window, _cents_to_dollars(args.on_fill),
                            _cents_to_dollars(args.on_fill_offset),
                            args.on_fill_offset_perc))
    elif args.cmd == "orders":
        asyncio.run(cmd_orders(args.coin))
    elif args.cmd == "cancel":
        asyncio.run(cmd_cancel(args.oid))
    elif args.cmd == "cancel-all":
        asyncio.run(cmd_cancel_all(args.coin))
    elif args.cmd == "snipe":
        asyncio.run(cmd_snipe(args.coin, args.side, args.qty,
                              _cents_to_dollars(args.discount),
                              _cents_to_dollars(args.level), args.window,
                              _cents_to_dollars(args.on_fill),
                              _cents_to_dollars(args.on_fill_offset),
                              args.on_fill_offset_perc))
    elif args.cmd == "positions":
        asyncio.run(cmd_positions(args.coin))
    elif args.cmd == "sell":
        asyncio.run(cmd_sell(args.coin, args.side, args.qty,
                             _cents_to_dollars(getattr(args, 'limit', None)),
                             args.window))
    elif args.cmd == "dump":
        asyncio.run(cmd_dump(args.coin))
    elif args.cmd == "watch":
        asyncio.run(cmd_watch(args.coin, args.action, args.side, args.qty,
                              args.target, _cents_to_dollars(args.limit),
                              _cents_to_dollars(args.discount),
                              _cents_to_dollars(args.on_fill),
                              _cents_to_dollars(args.on_fill_offset),
                              args.on_fill_offset_perc))
    elif args.cmd == "ticker":
        asyncio.run(cmd_ticker(args.coin))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
