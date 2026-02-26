import kalshi_python
from kalshi_python.api import EventsApi
from kalshi_python.configuration import Configuration
import csv
from datetime import datetime
import time
import os

config = Configuration(host="https://api.elections.kalshi.com/trade-api/v2")
config.api_key_id = "1d3dba8d-6a07-4936-80bb-d697524fc501"

with open("/Users/adamjohnson-hill/.kalshi/private_key.pem", "r") as f:
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

master_file = "/Users/adamjohnson-hill/Desktop/kalshi-analysis/master_spreads.csv"
file_exists = os.path.exists(master_file)

with open(master_file, "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    if not file_exists:
        writer.writeheader()
    writer.writerows(results)

print(f"{datetime.now().isoformat()} — Appended {len(results)} markets to master_spreads.csv")
