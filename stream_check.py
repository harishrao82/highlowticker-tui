#!/usr/bin/env python3
"""
stream_check.py — raw Tradier SSE stream monitor.

Connects directly to the Tradier stream and prints every event so you can see
exactly what is (or isn't) coming through.

Usage:
    cd highlow-tui
    TRADIER_ACCESS_TOKEN=<your_token> python ../stream_check.py
    # or if token is already in your shell environment:
    python stream_check.py
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import httpx
except ImportError:
    print("Missing httpx — run: pip install httpx", file=sys.stderr)
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "highlow-tui", ".env"))
except ImportError:
    pass

TOKEN       = os.getenv("TRADIER_ACCESS_TOKEN", "").strip()
REST_BASE   = "https://api.tradier.com/v1"
STREAM_BASE = "https://stream.tradier.com/v1"
WATCH       = ["SPY", "AAPL", "MSFT", "NVDA", "TSLA"]   # small set for quick test
_ET         = ZoneInfo("America/New_York")


def now_str() -> str:
    return datetime.now(tz=_ET).strftime("%H:%M:%S")


async def get_session_id(token: str) -> str:
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{REST_BASE}/markets/events/session",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        r.raise_for_status()
        return r.json()["stream"]["sessionid"]


async def stream_events(token: str, session_id: str, symbols: list[str]) -> None:
    url = f"{STREAM_BASE}/markets/events"
    params = {
        "sessionid": session_id,
        "symbols":   ",".join(symbols),
        "filter":    "trade,quote",
        "linebreak": "true",
    }
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    count = 0
    t0    = time.time()

    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream("GET", url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            print(f"[{now_str()}] Connected — listening for events on: {', '.join(symbols)}\n")
            async for line in resp.aiter_lines():
                line = line.strip()
                if not line:
                    continue
                count += 1
                elapsed = time.time() - t0
                try:
                    ev = json.loads(line)
                    ev_type = ev.get("type", "?")
                    sym     = ev.get("symbol", ev.get("s", "?"))
                    price   = ev.get("price", ev.get("bid", ev.get("ask", "")))
                    ts_str  = now_str()
                    print(f"[{ts_str}] #{count:>5}  {ev_type:<8}  {sym:<6}  {price}  "
                          f"  (rate: {count/elapsed:.1f}/s)")
                except json.JSONDecodeError:
                    print(f"[{now_str()}] raw: {line}")


async def main() -> None:
    if not TOKEN:
        print("Error: TRADIER_ACCESS_TOKEN not set.", file=sys.stderr)
        print("  export TRADIER_ACCESS_TOKEN=your_token_here", file=sys.stderr)
        sys.exit(1)

    print(f"[{now_str()}] Getting stream session...")
    session_id = await get_session_id(TOKEN)
    print(f"[{now_str()}] Session: {session_id}")
    await stream_events(TOKEN, session_id, WATCH)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
