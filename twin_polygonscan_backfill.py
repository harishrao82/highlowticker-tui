"""twin_polygonscan_backfill.py — pull Twin's historical Polymarket trades.

Uses Etherscan V2 (chainid=137 for Polygon) to query OrderFilled events with
maker=Twin going back N days.  Etherscan keeps full archive — handles old
blocks the free public RPC has pruned.

Asset_id → market metadata:
  Bootstrap from our shadow log (~/.twin_live_trades.jsonl) by joining tx
  hashes.  Asset_ids that appear in matched trades give us the asset_id →
  slug mapping; we apply that mapping to events from before our shadow
  log started.

Output: ~/.twin_polygonscan_backfill.jsonl
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

ET = ZoneInfo("America/New_York")
API_KEY = os.environ.get("ETHERSCAN_API_KEY", "P8ZIET4YHQMVB1MNKKKREBKSGBRN85711U")
API_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 137   # Polygon

CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

TWIN_WALLET = "0x3a847382ad6fff9be1db4e073fd9b869f6884d44"
TWIN_TOPIC = "0x" + "0" * 24 + TWIN_WALLET[2:]

SHADOW_LOG = Path.home() / ".twin_live_trades.jsonl"
OUTPUT_LOG = Path.home() / ".twin_polygonscan_backfill.jsonl"

DAYS_BACK = float(sys.argv[1]) if len(sys.argv) > 1 else 7.0

# Etherscan caps logs at 1000 results per call — paginate by block range.
# Twin makes ~5000-7000 events/day, so ~500/h.  Use 5000-block chunks (~3h).
CHUNK_BLOCKS = 5000


async def get_logs(client: httpx.AsyncClient, from_block: int, to_block: int):
    for attempt in range(3):
        try:
            r = await client.get(API_URL, params={
                "chainid": CHAIN_ID,
                "module": "logs", "action": "getLogs",
                "fromBlock": from_block, "toBlock": to_block,
                "address": CTF_EXCHANGE,
                "topic0": ORDER_FILLED_TOPIC,
                "topic2": TWIN_TOPIC,
                "topic0_2_opr": "and",
                "apikey": API_KEY,
            }, timeout=30)
            data = r.json()
            msg = data.get("message", "")
            if msg in ("OK", "No records found"):
                return data.get("result", []) or []
            if "rate limit" in str(data).lower():
                await asyncio.sleep(1.0)
                continue
            print(f"  unexpected: {msg} {str(data)[:200]}")
            return []
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1.0)
                continue
            print(f"  err: {e}")
            return []
    return []


def decode_event(log: dict) -> dict | None:
    data = log["data"][2:]
    fields = [int(data[i:i+64], 16) for i in range(0, len(data), 64)]
    maker_asset, taker_asset, maker_amt, taker_amt, fee = fields[:5]
    if maker_asset == 0 and taker_amt > 0:
        price = maker_amt / taker_amt
        size = taker_amt / 1_000_000
        notional = maker_amt / 1_000_000
        outcome_asset = taker_asset
        side = "BUY"
    elif taker_asset == 0 and maker_amt > 0:
        price = taker_amt / maker_amt
        size = maker_amt / 1_000_000
        notional = taker_amt / 1_000_000
        outcome_asset = maker_asset
        side = "SELL"
    else:
        return None

    block_num = int(log["blockNumber"], 16)
    ts = int(log.get("timeStamp", "0x0"), 16)
    return {
        "tx": log["transactionHash"],
        "block_num": block_num,
        "trade_ts": ts,
        "asset_id": str(outcome_asset),
        "side": side,
        "price": round(price, 4),
        "size": round(size, 4),
        "notional": round(notional, 2),
        "fee": fee / 1_000_000,
    }


def load_shadow_mapping():
    out = {}
    if not SHADOW_LOG.exists():
        return out
    with open(SHADOW_LOG) as f:
        for line in f:
            try: r = json.loads(line)
            except: continue
            tx = r.get("tx")
            slug = r.get("slug")
            if not tx or not slug: continue
            out[tx.lower()] = {
                "slug": slug,
                "outcome": r.get("outcome"),
                "coin": r.get("coin"),
                "market_type": r.get("market_type"),
                "window_start_ts": r.get("window_start_ts"),
            }
    return out


async def main():
    print(f"Twin backfill (Etherscan V2) — last {DAYS_BACK} days")

    async with httpx.AsyncClient(timeout=30) as client:
        # Get current block
        r = await client.get(API_URL, params={
            "chainid": CHAIN_ID, "module": "proxy",
            "action": "eth_blockNumber", "apikey": API_KEY,
        }, timeout=15)
        current = int(r.json()["result"], 16)
        blocks_back = int(DAYS_BACK * 86400 / 2)
        from_block = current - blocks_back
        print(f"  Block range: {from_block:,} -> {current:,}  ({blocks_back:,} blocks)")

        shadow_tx_slug = load_shadow_mapping()
        print(f"  Shadow log mappings: {len(shadow_tx_slug):,}")

        # Iterate chunks
        all_events = []
        cur = from_block
        chunk_n = 0
        while cur < current:
            chunk_end = min(cur + CHUNK_BLOCKS - 1, current)
            logs = await get_logs(client, cur, chunk_end)
            all_events.extend(logs)
            chunk_n += 1
            if chunk_n % 5 == 0 or len(logs) >= 1000:
                t_first = int(logs[0].get("timeStamp", "0x0"), 16) if logs else 0
                t_first_str = datetime.fromtimestamp(t_first, tz=ET).strftime("%m/%d %I:%M%p") if t_first else "?"
                print(f"  chunk {chunk_n}: blocks {cur:,}-{chunk_end:,}  "
                      f"+{len(logs)} events  total={len(all_events):,}  first_ts={t_first_str}",
                      flush=True)
            # Etherscan returns max 1000 — if we hit cap, narrow range to avoid missing
            if len(logs) >= 1000:
                print(f"    (hit 1000-event cap, splitting chunk)")
                # Split: redo this range in 2 halves
                mid = (cur + chunk_end) // 2
                logs2 = await get_logs(client, cur, mid)
                logs3 = await get_logs(client, mid + 1, chunk_end)
                # Replace what we just added
                all_events = all_events[:-len(logs)] + logs2 + logs3
                print(f"    after split: +{len(logs2)+len(logs3)} events")
            cur = chunk_end + 1
            await asyncio.sleep(0.25)   # be nice (5/sec limit)

        print(f"\n  Fetched {len(all_events):,} OrderFilled events")

        # Decode + match to shadow log for asset_id mapping
        decoded = []
        asset_id_meta = {}
        for log in all_events:
            d = decode_event(log)
            if not d: continue
            tx = d["tx"].lower()
            if tx in shadow_tx_slug:
                asset_id_meta[d["asset_id"]] = shadow_tx_slug[tx]
            decoded.append(d)

        print(f"  Decoded: {len(decoded):,}")
        print(f"  Asset_id mappings learned: {len(asset_id_meta):,}")

        # Apply mappings, write JSONL
        n_with_meta = 0
        out_lines = []
        for d in decoded:
            entry = dict(d)
            entry["source"] = "polygonscan"
            meta = asset_id_meta.get(d["asset_id"])
            if meta:
                entry.update({
                    "slug": meta.get("slug"),
                    "coin": meta.get("coin"),
                    "outcome": meta.get("outcome"),
                    "market_type": meta.get("market_type"),
                    "window_start_ts": meta.get("window_start_ts"),
                })
                if meta.get("window_start_ts"):
                    entry["elapsed_in_window"] = d["trade_ts"] - int(meta["window_start_ts"])
                n_with_meta += 1
            out_lines.append(json.dumps(entry))

        with open(OUTPUT_LOG, "w") as f:
            f.write("\n".join(out_lines) + "\n")

        print(f"\n=== Summary ===")
        print(f"Total events:           {len(decoded):,}")
        print(f"With market metadata:   {n_with_meta:,} ({n_with_meta/len(decoded)*100:.0f}%)")
        if decoded:
            ts_min = min(d["trade_ts"] for d in decoded)
            ts_max = max(d["trade_ts"] for d in decoded)
            print(f"Time range: {datetime.fromtimestamp(ts_min, tz=ET).strftime('%a %m/%d %I:%M%p')} -> "
                  f"{datetime.fromtimestamp(ts_max, tz=ET).strftime('%a %m/%d %I:%M%p ET')}")
        print(f"Output: {OUTPUT_LOG}")


if __name__ == "__main__":
    asyncio.run(main())
