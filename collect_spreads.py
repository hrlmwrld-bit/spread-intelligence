import kalshi_python
from kalshi_python.api import EventsApi
from kalshi_python.configuration import Configuration
import csv
import os
print("ENV VARS:", [k for k in os.environ.keys() if "KALSHI" in k])
from datetime import datetime
import time

config = Configuration(host="https://api.elections.kalshi.com/trade-api/v2")
config.api_key_id = os.environ.get("KALSHI_API_KEY", "1d3dba8d-6a07-4936-80bb-d697524fc501")

kalshi_private_key = os.environ.get("KALSHI_PRIVATE_KEY")
print(f"Key loaded: {bool(kalshi_private_key)}, length: {len(kalshi_private_key) if kalshi_private_key else 0}")
if kalshi_private_key:
    config.private_key_pem = kalshi_private_key.replace("\\n", "\n")
else:
    with open(os.path.expanduser("~/.kalshi/private_key.pem"), "r") as f:
        config.private_key_pem = f.read()

client = kalshi_python.ApiClient(config)
events_api = EventsApi(client)

results = []
cursor = None
total_pulled = 0

while total_pulled < 500:
    try:
        response = events_api.get_events(limit=50, status="open", with_nested_markets=True, cursor=cursor)
    except Exception as e:
        print(f"Error: {e}")
        break
    if not response.events:
        break
    for event in response.events:
        try:
            if event.markets:
                for market in event.markets:
                    try:
                        bid = market.yes_bid or 0
                        ask = market.yes_ask or 0
                        if bid > 0 and ask > 0:
                            midpoint = (bid + ask) / 2
                            spread = ask - bid
                            spread_pct = round((spread / midpoint) * 100, 2)
                            results.append({
                                "timestamp": datetime.now().isoformat(),
                                "event": event.title,
                                "series": event.series_ticker,
                                "ticker": market.ticker,
                                "bid": bid,
                                "ask": ask,
                                "midpoint": midpoint,
                                "spread": spread,
                                "spread_pct": spread_pct,
                                "volume": market.volume or 0,
                                "volume_24h": market.volume_24h or 0,
                                "last_price": market.last_price or 0,
                                "open_time": str(market.open_time) if market.open_time else "",
                                "close_time": str(market.close_time) if market.close_time else "",
                                "status": market.status or "",
                            })
                    except Exception:
                        continue
        except Exception:
            continue
    cursor = response.cursor
    total_pulled += len(response.events)
    time.sleep(1)
    if not cursor:
        break

master_file = os.environ.get("CSV_PATH", "/data/master_spreads.csv")
file_exists = os.path.exists(master_file)

with open(master_file, "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    if not file_exists:
        writer.writeheader()
    writer.writerows(results)

print(f"{datetime.now().isoformat()} — Appended {len(results)} markets to master_spreads.csv")
