#!/usr/bin/env python3
import http.server
import json
import csv
import os
from collections import defaultdict
from datetime import datetime

CSV_PATH = os.path.expanduser("~/Desktop/kalshi-analysis/master_spreads.csv")
PORT = 8765

CATEGORY_MAP = {
    # Politics
    "KXHOUS": "Politics", "KXRHOU": "Politics", "KXDHOU": "Politics",
    "KXSENA": "Politics", "KXDSEN": "Politics", "KXPRES": "Politics",
    "KXVPRE": "Politics", "KXGOV": "Politics", "KXGOVC": "Politics",
    "KXGOVP": "Politics", "KXGOVT": "Politics", "KXGORT": "Politics",
    "KXPOLI": "Politics", "KXPRIM": "Politics", "KXELEC": "Politics",
    "KXBALL": "Politics", "KXCONG": "Politics", "KXATTY": "Politics",
    "KXIMPE": "Politics", "KXVETO": "Politics", "KXPOLA": "Politics",
    "KXUKPA": "Politics", "KXPARL": "Politics", "KXSCOT": "Politics",
    "KXFREN": "Politics", "KXITAL": "Politics", "KXGREE": "Politics",
    "KXARGE": "Politics", "KXBRAZ": "Politics", "KXMEXIC": "Politics",
    "KXCANA": "Politics", "KXGHAN": "Politics", "KXKENY": "Politics",
    "KXNIGE": "Politics", "KXZELE": "Politics", "KXPUTI": "Politics",
    "KXTRUM": "Politics", "KXBIDE": "Politics", "KXKAMA": "Politics",
    "KXDJTW": "Politics", "KXJOHN": "Politics", "KXDOEL": "Politics",
    "KXDOED": "Politics", "KXDEMO": "Politics", "KXJAN6": "Politics",
    "KXFREE": "Politics", "KX2028": "Politics", "GOVPAR": "Politics",
    "SENATE": "Politics", "POWER": "Politics", "CONTRO": "Politics",
    "KXSENA": "Politics", "KXSTAT": "Politics", "KXTARI": "Politics",
    "KXDEPO": "Politics", "KXIMMI": "Politics", "KXBANT": "Politics",
    "KXFIRS": "Politics", "KXTERM": "Politics", "KXELON": "Politics",
    "KXMUSK": "Politics", "KXDEEL": "Politics", "KXWITH": "Politics",
    "KXTAFT": "Politics", "KXOAIA": "Politics", "KXOAID": "Politics",
    "KXH1B": "Politics", "KXRIPP": "Politics", "KXIRSC": "Politics",
    "KXUSAK": "Politics", "KXUSAE": "Politics", "KXINDA": "Politics",
    "KXINDI": "Politics", "KXTAIW": "Politics", "KXCHIN": "Politics",
    "KXUKRA": "Politics", "KXWARS": "Politics", "KXRUSS": "Politics", "KXPRES": "Politics", "KXHOUS": "Politics", "KXVPRE": "Politics", "KXATTY": "Politics", "KXCABO": "Politics",
    "KXPRES": "Politics", "KXHOUS": "Politics", "KXRHOU": "Politics",
    "KXDHOU": "Politics", "KXATTY": "Politics", "KXVPRE": "Politics",
    "KXCABO": "Politics", "KXCAGO": "Politics", "KXGREE": "Politics",

    # Macro/Economic
    "KXFED": "Macro/Economic", "KXFEDC": "Macro/Economic", "KXFEDD": "Macro/Economic",
    "KXFEDE": "Macro/Economic", "KXCPI": "Macro/Economic", "KXLCPI": "Macro/Economic",
    "KXGDPY": "Macro/Economic", "KXGDPS": "Macro/Economic", "KXGDPU": "Macro/Economic",
    "KXUNEM": "Macro/Economic", "KXU3MA": "Macro/Economic", "KXINFL": "Macro/Economic",
    "KXDEBT": "Macro/Economic", "KXBOND": "Entertainment", "KXRATE": "Macro/Economic",
    "KXCOST": "Macro/Economic", "KXCONS": "Macro/Economic", "KXHOWM": "Macro/Economic",
    "KXNUMS": "Macro/Economic", "KXDATA": "Macro/Economic", "KXRECP": "Macro/Economic",
    "KXMEDI": "Macro/Economic", "KXINSU": "Macro/Economic", "KXTXRS": "Macro/Economic",
    "KXTXSE": "Macro/Economic", "KXFTA": "Macro/Economic", "KXFTAP": "Macro/Economic",

    # Sports
    "KXNBA": "Sports", "KXNBAS": "Sports", "KXNBAT": "Sports",
    "KXNFL": "Sports", "KXNFLA": "Sports", "KXNFLD": "Sports",
    "KXNFLM": "Sports", "KXNFLN": "Sports", "KXNFLO": "Sports",
    "KXNFLP": "Sports", "KXSUPE": "Sports", "KXSB": "Sports",
    "KXMLB": "Sports", "KXNHL": "Sports", "KXNCAA": "Sports",
    "KXPGA": "Sports", "KXUFC": "Sports", "KXMLS": "Sports",
    "KXESPO": "Sports", "KXNASCAR": "Sports", "KXTENNI": "Sports",
    "KXNDJO": "Sports", "KXSCOU": "Sports", "KXPHIL": "Sports",
    "KXRANK": "Sports", "KXROLE": "Sports", "KXXISU": "Sports",
    "KXJOIN": "Sports", "KXEOTR": "Sports", "KXSPOR": "Sports",
    "KXAUST": "Sports", "KXLOND": "Sports", "KXNETW": "Sports",

    # Entertainment
    "KXRECO": "Entertainment", "KXGRAM": "Entertainment", "KXOSC": "Entertainment", "KXMOVC": "Entertainment",
    "KXMOVN": "Entertainment", "KXMOVW": "Entertainment", "KXTVSE": "Entertainment",
    "KXSHOW": "Entertainment", "KXSNL": "Entertainment", "KXPERF": "Entertainment", "KXPERFORM": "Entertainment",
    "KXSWIF": "Entertainment", "KXBEYO": "Entertainment", "KXPOPC": "Entertainment",
    "KXMUSI": "Entertainment", "KXLIPA": "Entertainment", "KXMAYO": "Entertainment",
    "KXEURE": "Entertainment", "KXESVI": "Entertainment", "KXACTO": "Entertainment",
    "KXTAYL": "Entertainment", "KXSONI": "Entertainment", "KXOBER": "Entertainment",
    "KXGTA6": "Entertainment", "KXVENU": "Entertainment", "KXRECO": "Entertainment",

    # Tech/IPO
    "KXIPO": "Tech/IPO", "KXIPOA": "Tech/IPO", "KXIPOB": "Tech/IPO",
    "KXIPOC": "Tech/IPO", "KXIPOD": "Tech/IPO", "KXIPOF": "Tech/IPO",
    "KXIPOG": "Tech/IPO", "KXIPOO": "Tech/IPO", "KXIPOR": "Tech/IPO",
    "KXIPOS": "Tech/IPO", "KXIPOW": "Tech/IPO", "KXAMAZ": "Tech/IPO",
    "KXAPPL": "Tech/IPO", "KXROBO": "Tech/IPO", "KXYTUB": "Tech/IPO",
    "KXCABL": "Tech/IPO", "KXNEXT": "Tech/IPO", "KXARTI": "Tech/IPO",
    "KXMACR": "Tech/IPO",

    # Science/Tech
    "KXSCI": "Science/Tech", "KXFUSI": "Science/Tech", "KXSPAC": "Science/Tech",
    "KXMARS": "Science/Tech", "KXMOON": "Science/Tech", "KXSTAR": "Science/Tech",
    "KXERUP": "Science/Tech", "KXEART": "Science/Tech", "KXWARM": "Science/Tech",
    "KXCO2L": "Science/Tech", "KXAFRI": "Science/Tech",
}

def categorize(series):
    s = series.upper()
    # Sort by prefix length descending so longer prefixes match first
    for prefix in sorted(CATEGORY_MAP.keys(), key=len, reverse=True):
        if s.startswith(prefix):
            return CATEGORY_MAP[prefix]
    return "Other"

def load_data():
    if not os.path.exists(CSV_PATH):
        return {"error": "CSV not found", "categories": [], "markets": [], "last_updated": None}

    markets_by_ticker = {}

    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row["ticker"]
            # Keep most recent entry per ticker
            markets_by_ticker[ticker] = row

    markets = list(markets_by_ticker.values())

    # Get most recent timestamp
    timestamps = [m["timestamp"] for m in markets if m.get("timestamp")]
    last_updated = max(timestamps) if timestamps else None

    # Compute category stats
    cat_data = defaultdict(list)
    for m in markets:
        try:
            spread_pct = float(m["spread_pct"])
            cat = categorize(m["series"])
            cat_data[cat].append({
                "event": m["event"],
                "ticker": m["ticker"],
                "series": m["series"],
                "bid": float(m["bid"]),
                "ask": float(m["ask"]),
                "mid": float(m["midpoint"]),
                "spread_pct": spread_pct,
                "cat": cat,
            })
        except (ValueError, KeyError):
            continue

    categories = []
    for cat, items in cat_data.items():
        spreads = sorted([i["spread_pct"] for i in items])
        n = len(spreads)
        mean = sum(spreads) / n
        median = spreads[n // 2] if n % 2 == 1 else (spreads[n//2 - 1] + spreads[n//2]) / 2
        categories.append({
            "name": cat,
            "mean": round(mean, 2),
            "median": round(median, 2),
            "n": n,
        })

    # Sort by mean spread ascending
    categories.sort(key=lambda x: x["mean"])

    # All markets for tables
    all_markets = []
    for items in cat_data.values():
        all_markets.extend(items)

    return {
        "categories": categories,
        "markets": all_markets,
        "last_updated": last_updated,
        "total": len(all_markets),
    }

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            data = load_data()
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]}")

if __name__ == "__main__":
    server = http.server.HTTPServer(("localhost", PORT), Handler)
    print(f"✓ Kalshi data server running at http://localhost:{PORT}/data")
    print(f"✓ Reading from: {CSV_PATH}")
    print(f"  Press Ctrl+C to stop\n")
    server.serve_forever()
