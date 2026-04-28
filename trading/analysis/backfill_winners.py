#!/usr/bin/env python3
"""
backfill_winners.py — fetch missing winners for closed unlabeled windows.

Queries ~/.btc_windows.db for windows with no winner that closed >5 min ago,
then calls Kalshi REST for each ticker and writes the result.
"""
import asyncio
import base64
import os
import sqlite3
import time
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

DB_PATH  = Path.home() / ".btc_windows.db"
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

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


def _get_unlabeled() -> list[tuple[int, str, int]]:
    """Return (id, ticker, window_start_ts) for closed windows missing a winner."""
    # window closes at window_start_ts + 900; include if closed more than 5 min ago
    cutoff = int(time.time()) - 300
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT id, ticker, window_start_ts FROM windows "
        "WHERE winner IS NULL AND window_start_ts + 900 < ? "
        "ORDER BY window_start_ts",
        (cutoff,),
    ).fetchall()
    con.close()
    return rows


def _write_winner(wid: int, ticker: str, winner: str) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE windows SET winner=? WHERE id=?", (winner, wid))
    con.commit()
    con.close()


async def _fetch_result(client: httpx.AsyncClient, ticker: str) -> str | None:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(
            f"{KALSHI_BASE}/markets/{ticker}",
            headers=_headers("GET", path),
            timeout=10,
        )
        if r.status_code == 200:
            result = r.json().get("market", {}).get("result", "")
            return result if result in ("yes", "no") else None
        else:
            console.print(f"  [dim yellow]{ticker}: HTTP {r.status_code}[/dim yellow]")
    except Exception as e:
        console.print(f"  [yellow]{ticker}: {e}[/yellow]")
    return None


async def main() -> None:
    rows = _get_unlabeled()
    if not rows:
        console.print("[green]No unlabeled closed windows — nothing to do.[/green]")
        return

    console.print(f"[bold cyan]Backfilling {len(rows)} unlabeled windows…[/bold cyan]\n")

    filled = 0
    missing = 0

    # Batch in chunks of 20 to avoid hammering the API
    async with httpx.AsyncClient() as client:
        for i in range(0, len(rows), 20):
            batch = rows[i:i+20]
            tasks = [_fetch_result(client, ticker) for _, ticker, _ in batch]
            results = await asyncio.gather(*tasks)

            for (wid, ticker, _), result in zip(batch, results):
                if result:
                    _write_winner(wid, ticker, result)
                    label = "[green]YES[/green]" if result == "yes" else "[red]NO[/red]"
                    console.print(f"  {ticker}  →  {label}")
                    filled += 1
                else:
                    console.print(f"  [dim]{ticker}  →  no result yet[/dim]")
                    missing += 1

            if i + 20 < len(rows):
                await asyncio.sleep(0.5)   # gentle rate limit

    console.print(f"\n[bold]Done.[/bold]  filled={filled}  still-missing={missing}")

    # Summary by coin
    con = sqlite3.connect(DB_PATH)
    rows2 = con.execute("""
        SELECT substr(ticker, 1, instr(ticker, '-')-1) as series,
               COUNT(*) as total,
               SUM(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END) as labeled,
               SUM(CASE WHEN winner='yes' THEN 1 ELSE 0 END) as yes_wins,
               SUM(CASE WHEN winner='no'  THEN 1 ELSE 0 END) as no_wins
        FROM windows GROUP BY series ORDER BY series
    """).fetchall()
    con.close()
    console.print("\n[bold cyan]Updated label counts:[/bold cyan]")
    for r in rows2:
        console.print(f"  {r[0]:12s}  total={r[1]:3d}  labeled={r[2]:3d}  yes={r[3]}  no={r[4]}")


if __name__ == "__main__":
    asyncio.run(main())
