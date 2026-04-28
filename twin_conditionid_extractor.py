"""twin_conditionid_extractor.py — fill in unmapped Twin asset_ids by extracting
the conditionId from on-chain PositionSplit events, then looking up the slug
via clob.polymarket.com/markets/{conditionId}.

Saves new mappings into ~/.polymarket_crypto_market_map.json.
"""
import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

import httpx

ETHERSCAN_KEY = "P8ZIET4YHQMVB1MNKKKREBKSGBRN85711U"
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
CLOB_BASE = "https://clob.polymarket.com"

POSITION_SPLIT_TOPIC = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
CONDITIONAL_TOKENS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

ASSET_MAP_PATH = Path.home() / ".polymarket_crypto_market_map.json"
BACKFILL_PATH = Path.home() / ".twin_polygonscan_backfill.jsonl"


async def fetch_receipt(client: httpx.AsyncClient, tx: str) -> dict | None:
    for attempt in range(3):
        try:
            r = await client.get(ETHERSCAN_URL, params={
                "chainid": 137, "module": "proxy",
                "action": "eth_getTransactionReceipt",
                "txhash": tx, "apikey": ETHERSCAN_KEY,
            }, timeout=15)
            data = r.json()
            result = data.get("result")
            if isinstance(result, dict):
                return result
            # rate-limit string or other error
            await asyncio.sleep(0.5)
        except Exception:
            await asyncio.sleep(0.5)
    return None


def find_condition_id(receipt: dict, target_asset_id: str) -> str | None:
    """Find the conditionId for the market this trade was on.

    Polymarket batches multiple trades per tx — receipt may have several
    PositionSplit events, each with a different conditionId.  We need the
    one whose splits produced our target_asset_id (the outcome token).

    Strategy: scan TransferSingle events for our asset_id (tells us which
    sub-batch contains it), then take the closest preceding PositionSplit's
    conditionId.
    """
    logs = receipt.get("logs", [])
    target_int = int(target_asset_id)
    target_hex = hex(target_int)[2:].rjust(64, "0").lower()

    # Walk logs in order. Track the most recent PositionSplit conditionId.
    # When we see a TransferSingle whose ID matches our asset_id, return that.
    last_split_cond = None
    TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
    for log in logs:
        topics = log.get("topics", [])
        if not topics: continue
        t0 = topics[0].lower()
        addr = log.get("address", "").lower()
        if addr != CONDITIONAL_TOKENS:
            continue
        if t0 == POSITION_SPLIT_TOPIC.lower() and len(topics) >= 4:
            last_split_cond = topics[3]
        elif t0 == TRANSFER_SINGLE.lower():
            # Data: [id (32 bytes), value (32 bytes)]
            data_hex = log.get("data", "")[2:]
            if len(data_hex) >= 64:
                id_hex = data_hex[:64]
                if id_hex.lower() == target_hex and last_split_cond:
                    return last_split_cond

    # Fallback: just return the FIRST PositionSplit conditionId
    for log in logs:
        topics = log.get("topics", [])
        if (log.get("address", "").lower() == CONDITIONAL_TOKENS
                and len(topics) >= 4
                and topics[0].lower() == POSITION_SPLIT_TOPIC.lower()):
            return topics[3]
    return None


async def fetch_market_by_condition(client: httpx.AsyncClient, cond: str) -> dict | None:
    for attempt in range(3):
        try:
            r = await client.get(f"{CLOB_BASE}/markets/{cond}", timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def main():
    print("Twin conditionId extractor — fill in unmapped asset_ids")
    print()

    # Load existing map + backfill events
    asset_map = json.loads(ASSET_MAP_PATH.read_text()) if ASSET_MAP_PATH.exists() else {}
    print(f"  Existing asset_id map: {len(asset_map):,}")

    events = []
    with open(BACKFILL_PATH) as f:
        for line in f:
            try: events.append(json.loads(line))
            except: pass

    # Find unmapped asset_id → one tx for each
    aid_to_tx = {}    # asset_id → (tx, block)
    for r in events:
        aid = r.get("asset_id")
        if not aid or aid in asset_map: continue
        if aid in aid_to_tx: continue
        aid_to_tx[aid] = (r["tx"], r["block_num"])

    print(f"  Unmapped asset_ids: {len(aid_to_tx):,}")
    print()

    # Incremental save: every N receipts, persist new mappings to disk
    SAVE_EVERY = 200
    aid_to_cond = {}
    cond_to_market_cache = {}     # cache CLOB results

    async def save_progress():
        ASSET_MAP_PATH.write_text(json.dumps(asset_map))

    async with httpx.AsyncClient(timeout=20) as client:
        items = list(aid_to_tx.items())
        for i, (aid, (tx, _block)) in enumerate(items):
            receipt = await fetch_receipt(client, tx)
            if receipt:
                cond = find_condition_id(receipt, aid)
                if cond:
                    aid_to_cond[aid] = cond
                    # Look up market immediately so we can save incrementally
                    if cond not in cond_to_market_cache:
                        m = await fetch_market_by_condition(client, cond)
                        cond_to_market_cache[cond] = m
                        await asyncio.sleep(0.15)
                    m = cond_to_market_cache.get(cond)
                    if m:
                        slug = m.get("market_slug", "")
                        if slug:
                            for tok in m.get("tokens", []):
                                if str(tok.get("token_id", "")) == aid:
                                    asset_map[aid] = {
                                        "slug": slug,
                                        "outcome": tok.get("outcome"),
                                        "condition_id": cond,
                                    }
                                    break
            if (i+1) % 100 == 0:
                resolved_clob = sum(1 for v in cond_to_market_cache.values() if v)
                print(f"  receipts {i+1}/{len(items)}  conds: {len(aid_to_cond)}  "
                      f"clob: {resolved_clob}/{len(cond_to_market_cache)}",
                      flush=True)
            if (i+1) % SAVE_EVERY == 0:
                await save_progress()
                print(f"    [saved progress: {len(asset_map):,} mappings]", flush=True)
            await asyncio.sleep(0.22)   # ~4.5 req/sec

    new_mappings = sum(1 for aid in aid_to_cond
                       if aid in asset_map
                       and asset_map[aid].get("condition_id") in cond_to_market_cache)

    print()
    print(f"=== Summary ===")
    print(f"Receipts fetched:    {len(aid_to_tx):,}")
    print(f"ConditionIds extracted: {len(new_conds):,}")
    print(f"Markets resolved:    {len(cond_to_market):,}")
    print(f"NEW asset_id mappings: {new_mappings:,}")
    print(f"Total asset_ids in map: {len(asset_map):,}")

    ASSET_MAP_PATH.write_text(json.dumps(asset_map))
    print(f"Saved: {ASSET_MAP_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
