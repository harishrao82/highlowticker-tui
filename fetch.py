import asyncio
import httpx
from datetime import datetime

# --- Configuration ---
# The target wallet address for our observations
TARGET_ADDRESS = "0xe1d6b51521bd4365769199f392f9818661bd907"
# The API endpoint dedicated to user activity
API_URL = f"https://data-api.polymarket.com/activity?user={TARGET_ADDRESS}&limit=10"

async def monitor_whale_activity():
    """
    A sophisticated monitor for tracking specific wallet executions 
    on the Polymarket exchange.
    """
    print(f"[*] Commencing vigilance for wallet: {TARGET_ADDRESS}")
    print("[*] Polling interval established at 2.0 seconds. Good luck, sir.\n")
    
    last_trade_id = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                # We request the most recent activity from the Data API
                response = await client.get(API_URL)
                
                if response.status_code == 200:
                    trades = response.json()
                    
                    if not trades:
                        await asyncio.sleep(2)
                        continue

                    # The first entry in the list is the most recent
                    latest_trade = trades[0]
                    # Identifying the trade by its unique transaction hash or ID
                    current_trade_id = latest_trade.get("transaction_hash") or latest_trade.get("id")

                    # If this ID differs from our last record, new activity has occurred
                    if last_trade_id and current_trade_id != last_trade_id:
                        for trade in trades:
                            trade_id = trade.get("transaction_hash") or trade.get("id")
                            if trade_id == last_trade_id:
                                break
                            
                            # Extracting the pertinent details of the execution
                            side = trade.get("side", "N/A").upper()
                            size = trade.get("size", "0")
                            price = trade.get("price", "0")
                            market = trade.get("market_title", "Unknown Market")
                            timestamp = datetime.now().strftime("%H:%M:%S")

                            # Presenting the findings with appropriate gravity
                            print(f"[{timestamp}] ⚡ NEW EXECUTION DETECTED")
                            print(f"    └─ Action: {side} {size} shares")
                            print(f"    └─ Price:  ${price}")
                            print(f"    └─ Market: {market}")
                            print(f"    " + "-"*40)

                    last_trade_id = current_trade_id
                
                # A 2-second interval ensures we remain inconspicuous yet swift
                await asyncio.sleep(2)

            except Exception as e:
                # Handling any unexpected irregularities in the connection gracefully
                print(f"[!] A minor interruption occurred: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_whale_activity())
    except KeyboardInterrupt:
        print("\n[*] The watch has ended. I bid you a productive day, sir.")