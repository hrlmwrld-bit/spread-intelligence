import requests
import csv
import os
from datetime import datetime
import time

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

results = []
cursor = None
total_pulled = 0

while total_pulled < 500:
    params = {"status": "open", "limit": 100}
    if cursor:
        params["cursor"] = cursor

    try:
        response = requests.get(f"{BASE_URL}/markets", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching markets: {e}")
        break

    markets = data.get("markets", [])
    if not markets:
        break

    for market in markets:
        try:
            bid = market.get("yes_bid") or 0
            ask = market.get("yes_ask") or 0
            if bid > 0 and ask > 0:
                midpoint = (bid + ask) / 2
                spread = ask - bid
                spread_pct = round((spread / midpoint) * 100, 2)
                results.append({
                    "timestamp": datetime.now().isoformat(),
                    "event": market.get("title", ""),
                    "series": market.get("series_ticker", ""),
                    "ticker": market.get("ticker", ""),
                    "bid": bid,
                    "ask": ask,
                    "midpoint": midpoint,
                    "spread": spread,
                    "spread_pct": spread_pct,
                    "volume": market.get("volume") or 0,
                    "volume_24h": market.get("volume_24h") or 0,
                    "last_price": market.get("last_price") or 0,
                    "open_time": market.get("open_time", ""),
                    "close_time": market.get("close_time", ""),
                    "status": market.get("status", ""),
                })
        except Exception:
            continue

    cursor = data.get("cursor")
    total_pulled += len(markets)
    time.sleep(0.5)
    if not cursor:
        break

if not results:
    print("No markets with valid spreads found.")
else:
    master_file = os.environ.get("CSV_PATH", "/data/master_spreads.csv")
    file_exists = os.path.exists(master_file)

    with open(master_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)

    print(f"{datetime.now().isoformat()} — Appended {len(results)} markets to master_spreads.csv")
