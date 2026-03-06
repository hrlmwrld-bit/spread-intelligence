import kalshi_python
from kalshi_python.api import EventsApi
from kalshi_python.configuration import Configuration
import csv
import os
from datetime import datetime
import time

# Patch kalshi_python to accept 'finalized' as a valid market status
try:
    import kalshi_python.models.market as _mkt
    _orig = _mkt.Market.model_validate.__func__
    def _patched(cls, data, *a, **kw):
        if isinstance(data, dict) and data.get('status') == 'finalized':
            data = dict(data)
            data['status'] = 'determined'
        return _orig(cls, data, *a, **kw)
    _mkt.Market.model_validate = classmethod(_patched)
except Exception as _e:
    print(f"Patch warning: {_e}")


config = Configuration(host="https://api.elections.kalshi.com/trade-api/v2")
config.api_key_id = os.environ.get("KALSHI_API_KEY", "1d3dba8d-6a07-4936-80bb-d697524fc501")
kalshi_private_key = os.environ.get("KALSHI_PRIVATE_KEY")
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
max_retries = 5

while total_pulled < 5000:
    retries = 0
    response = None
    while retries < max_retries:
        try:
            response = events_api.get_events(limit=200, status="open", with_nested_markets=True, cursor=cursor)
            break
        except Exception as e:
            err = str(e)
            if "429" in err or "too_many_requests" in err.lower():
                wait = 30 * (retries + 1)
                print(f"Rate limited, waiting {wait}s before retry {retries+1}/{max_retries}...")
                time.sleep(wait)
                retries += 1
            else:
                print(f"Error: {e}")
                if "validation error" in err.lower() or "finalized" in err.lower():
                    retries += 1
                    time.sleep(5)
                else:
                    break
    if response is None or not response.events:
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
    time.sleep(2)
    if not cursor:
        break

master_file = os.environ.get("CSV_PATH", "/data/master_spreads.csv")

if not results:
    print(f"{datetime.now().isoformat()} — No results to write (rate limited or empty response)")
else:
    file_exists = os.path.exists(master_file)
    with open(master_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)
    print(f"{datetime.now().isoformat()} — Appended {len(results)} markets to master_spreads.csv")
