#!/usr/bin/env python3
"""
Kalshi Spread Intelligence API
================================
Base URL: http://localhost:8766/v1

Endpoints:
  GET /v1/categories              — Summary stats by category
  GET /v1/markets                 — All current markets (filterable)
  GET /v1/markets/:ticker         — Single market detail + history
  GET /v1/history                 — Spread trends over time by category
  GET /v1/extremes                — Widest and tightest markets right now
  GET /v1/status                  — API health + dataset info

Query Parameters:
  category    Filter by category name (e.g. Sports, Politics)
  sort        Sort markets by: spread_pct, bid, ask, ticker (default: spread_pct)
  order       asc or desc (default: desc)
  limit       Number of results (default: 50, max: 500)
  min_spread  Minimum spread % filter
  max_spread  Maximum spread % filter
"""

import http.server
import threading
import subprocess
import sys
import json
import csv
import os
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta

CSV_PATH = os.environ.get("CSV_PATH", "/data/master_spreads.csv")
PORT = int(os.environ.get("PORT", 8766))

def _days_between(start_str, end_str):
    try:
        start = datetime.fromisoformat(start_str.replace("+00:00","").strip())
        end = datetime.fromisoformat(end_str.replace("+00:00","").strip())
        return (end - start).days
    except:
        return None

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
    "KXARGE": "Politics", "KXBRAZ": "Politics", "KXCANA": "Politics",
    "KXGHAN": "Politics", "KXKENY": "Politics", "KXNIGE": "Politics",
    "KXZELE": "Politics", "KXPUTI": "Politics", "KXTRUM": "Politics",
    "KXBIDE": "Politics", "KXKAMA": "Politics", "KXDJTW": "Politics",
    "KXJOHN": "Politics", "KXDOED": "Politics", "KXDEMO": "Politics",
    "KXJAN6": "Politics", "KXFREE": "Politics", "KX2028": "Politics",
    "GOVPAR": "Politics", "SENATE": "Politics", "POWER": "Politics",
    "CONTRO": "Politics", "KXSTAT": "Politics", "KXTARI": "Politics",
    "KXDEPO": "Politics", "KXBANT": "Politics", "KXFIRS": "Politics",
    "KXTERM": "Politics", "KXELON": "Politics", "KXMUSK": "Politics",
    "KXDEEL": "Politics", "KXWITH": "Politics", "KXTAFT": "Politics",
    "KXOAIA": "Politics", "KXOAID": "Politics", "KXH1B": "Politics",
    "KXRIPP": "Politics", "KXIRSC": "Politics", "KXUSAK": "Politics",
    "KXUSAE": "Politics", "KXINDI": "Politics", "KXTAIW": "Politics",
    "KXCHIN": "Politics", "KXWARS": "Politics", "KXRUSS": "Politics",
    "KXCABO": "Politics", "KXCAGO": "Politics",

    # Macro/Economic
    "KXFED":  "Macro/Economic", "KXFEDC": "Macro/Economic", "KXFEDD": "Macro/Economic",
    "KXFEDE": "Macro/Economic", "KXCPI":  "Macro/Economic", "KXLCPI": "Macro/Economic",
    "KXGDPY": "Macro/Economic", "KXGDPS": "Macro/Economic", "KXGDPU": "Macro/Economic",
    "KXU3MA": "Macro/Economic", "KXDEBT": "Macro/Economic", "KXBOND": "Entertainment",
    "KXCOST": "Macro/Economic", "KXCONS": "Macro/Economic", "KXHOWM": "Macro/Economic",
    "KXNUMS": "Macro/Economic", "KXDATA": "Macro/Economic", "KXMEDI": "Macro/Economic",
    "KXINSU": "Macro/Economic", "KXTXRS": "Macro/Economic", "KXTXSE": "Macro/Economic",
    "KXFTA":  "Macro/Economic", "KXFTAP": "Macro/Economic",

    # Sports
    "KXNBA":  "Sports", "KXNBAS": "Sports", "KXNBAT": "Sports",
    "KXNFL":  "Sports", "KXNFLA": "Sports", "KXNFLD": "Sports",
    "KXNFLM": "Sports", "KXNFLN": "Sports", "KXNFLO": "Sports",
    "KXNFLP": "Sports", "KXSUPE": "Sports", "KXSB":   "Sports",
    "KXMLB":  "Sports", "KXNHL":  "Sports", "KXNCAA": "Sports",
    "KXPGA":  "Sports", "KXUFC":  "Sports", "KXMLS":  "Sports",
    "KXNDJO": "Sports", "KXSCOU": "Sports", "KXPHIL": "Sports",
    "KXRANK": "Sports", "KXROLE": "Sports", "KXXISU": "Sports",
    "KXJOIN": "Sports", "KXEOTR": "Sports", "KXSPOR": "Sports",
    "KXAUST": "Sports", "KXLOND": "Sports", "KXNETW": "Sports",

    # Entertainment
    "KXGRAM": "Entertainment", "KXOSC":  "Entertainment", "KXMOVC": "Entertainment",
    "KXMOVN": "Entertainment", "KXMOVW": "Entertainment", "KXTVSE": "Entertainment",
    "KXSHOW": "Entertainment", "KXSNL":  "Entertainment", "KXPERF": "Entertainment", "KXPERFORM": "Entertainment",
    "KXSWIF": "Entertainment", "KXBEYO": "Entertainment", "KXPOPC": "Entertainment",
    "KXLIPA": "Entertainment", "KXMAYO": "Entertainment", "KXEURE": "Entertainment",
    "KXESVI": "Entertainment", "KXACTO": "Entertainment", "KXTAYL": "Entertainment",
    "KXSONI": "Entertainment", "KXOBER": "Entertainment", "KXGTA6": "Entertainment",
    "KXVENU": "Entertainment", "KXRECO": "Entertainment",

    # Tech/IPO
    "KXIPOA": "Tech/IPO", "KXIPOB": "Tech/IPO", "KXIPOC": "Tech/IPO",
    "KXIPOD": "Tech/IPO", "KXIPOF": "Tech/IPO", "KXIPOG": "Tech/IPO",
    "KXIPOO": "Tech/IPO", "KXIPOR": "Tech/IPO", "KXIPOS": "Tech/IPO",
    "KXIPOW": "Tech/IPO", "KXAMAZ": "Tech/IPO", "KXAPPL": "Tech/IPO",
    "KXROBO": "Tech/IPO", "KXYTUB": "Tech/IPO", "KXCABL": "Tech/IPO",
    "KXARTI": "Tech/IPO", "KXMACR": "Tech/IPO",

    # Science/Tech
    "KXFUSI": "Science/Tech", "KXSPAC": "Science/Tech", "KXMARS": "Science/Tech",
    "KXMOON": "Science/Tech", "KXSTAR": "Science/Tech", "KXERUP": "Science/Tech",
    "KXEART": "Science/Tech", "KXWARM": "Science/Tech", "KXCO2L": "Science/Tech",
}

def categorize(series):
    s = series.upper()
    for prefix in sorted(CATEGORY_MAP.keys(), key=len, reverse=True):
        if s.startswith(prefix):
            return CATEGORY_MAP[prefix]
    return "Other"

def load_all_rows():
    """Load all rows from master CSV."""
    if not os.path.exists(CSV_PATH):
        return []
    rows = []
    with open(CSV_PATH, "r") as f:
        for row in csv.DictReader(f):
            try:
                rows.append({
                    "timestamp": row["timestamp"],
                    "event":     row["event"],
                    "series":    row["series"],
                    "ticker":    row["ticker"],
                    "bid":       float(row["bid"]),
                    "ask":       float(row["ask"]),
                    "mid":       float(row["midpoint"]),
                    "spread":    float(row["spread"]),
                    "spread_pct": float(row["spread_pct"]),
                    "volume":     int(float(row.get("volume", 0) or 0)),
                    "volume_24h": int(float(row.get("volume_24h", 0) or 0)),
                    "last_price": int(float(row.get("last_price", 0) or 0)),
                    "open_time":  row.get("open_time", ""),
                    "close_time": row.get("close_time", ""),
                    "status": row.get("status", "active"),
                    "category":  categorize(row["series"]),
                    "days_to_close": _days_between(datetime.utcnow().isoformat(), row.get("close_time", "")),
                    "days_since_open": _days_between(row.get("open_time", ""), datetime.utcnow().isoformat()),
                })
            except (ValueError, KeyError):
                continue
    return rows

def get_latest_snapshot(rows):
    """Get most recent entry per ticker."""
    latest = {}
    for row in rows:
        t = row["ticker"]
        if t not in latest or row["timestamp"] > latest[t]["timestamp"]:
            latest[t] = row
    return [r for r in latest.values() if r.get("status", "active") != "finalized"]

def compute_category_stats(markets):
    """Compute mean/median/n by category."""
    cats = defaultdict(list)
    for m in markets:
        cats[m["category"]].append(m["spread_pct"])
    result = []
    for cat, spreads in cats.items():
        spreads_sorted = sorted(spreads)
        n = len(spreads_sorted)
        mean = round(sum(spreads_sorted) / n, 2)
        median = spreads_sorted[n // 2] if n % 2 == 1 else round(
            (spreads_sorted[n//2 - 1] + spreads_sorted[n//2]) / 2, 2)
        result.append({
            "category": cat,
            "mean_spread_pct": mean,
            "median_spread_pct": median,
            "n_markets": n,
            "min_spread_pct": round(min(spreads_sorted), 2),
            "max_spread_pct": round(max(spreads_sorted), 2),
        })
    return sorted(result, key=lambda x: x["mean_spread_pct"])

def compute_history(rows, days=30):
    """Compute daily category averages over time."""
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    recent = [r for r in rows if r["timestamp"] >= cutoff]

    # Group by date + category
    by_date_cat = defaultdict(list)
    for r in recent:
        date = r["timestamp"][:10]
        by_date_cat[(date, r["category"])].append(r["spread_pct"])

    result = defaultdict(list)
    for (date, cat), spreads in sorted(by_date_cat.items()):
        result[cat].append({
            "date": date,
            "mean_spread_pct": round(sum(spreads) / len(spreads), 2),
            "n_markets": len(spreads),
        })
    return dict(result)

def parse_qs(path):
    """Parse query string from path."""
    if "?" not in path:
        return {}
    qs = path.split("?", 1)[1]
    return dict(urllib.parse.parse_qsl(qs))

def filter_and_sort(markets, params):
    """Apply query param filters and sorting."""
    results = markets

    if "category" in params:
        results = [m for m in results if m["category"].lower() == params["category"].lower()]
    if "min_spread" in params:
        results = [m for m in results if m["spread_pct"] >= float(params["min_spread"])]
    if "max_spread" in params:
        results = [m for m in results if m["spread_pct"] <= float(params["max_spread"])]

    sort_key = params.get("sort", "spread_pct")
    if sort_key not in ("spread_pct", "bid", "ask", "mid", "ticker"):
        sort_key = "spread_pct"
    order = params.get("order", "desc") == "asc"
    results = sorted(results, key=lambda m: m.get(sort_key, 0), reverse=not order)

    try:
        limit = min(int(params.get("limit", 50)), 3000)
    except ValueError:
        limit = 50
    return results[:limit]

class APIHandler(http.server.BaseHTTPRequestHandler):

    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, message, status=400):
        self.send_json({"error": message, "status": status}, status)

    def do_OPTIONS(self):
        self.send_json({})

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        params = parse_qs(self.path.split("?")[1] if "?" in self.path else "")

        # Load data
        all_rows = load_all_rows()
        snapshot = get_latest_snapshot(all_rows)

        # ── GET /v1/status ──────────────────────────────────────────────────
        if path == "/v1/status":
            timestamps = sorted([r["timestamp"] for r in all_rows])
            self.send_json({
                "status": "ok",
                "version": "1.0.0",
                "dataset": {
                    "total_rows": len(all_rows),
                    "total_markets": len(snapshot),
                    "earliest_snapshot": timestamps[0] if timestamps else None,
                    "latest_snapshot": timestamps[-1] if timestamps else None,
                    "csv_path": CSV_PATH,
                },
                "endpoints": [
                    "GET /v1/status",
                    "GET /v1/categories",
                    "GET /v1/markets",
                    "GET /v1/markets/:ticker",
                    "GET /v1/history",
                    "GET /v1/extremes",
                ]
            })

        # ── GET /v1/categories ──────────────────────────────────────────────
        elif path == "/v1/categories":
            stats = compute_category_stats(snapshot)
            self.send_json({
                "last_updated": max((r["timestamp"] for r in snapshot), default=None),
                "total_markets": len(snapshot),
                "categories": stats,
            })

        # ── GET /v1/markets ─────────────────────────────────────────────────
        elif path == "/v1/markets":
            filtered = filter_and_sort(snapshot, params)
            self.send_json({
                "last_updated": max((r["timestamp"] for r in snapshot), default=None),
                "total_results": len(filtered),
                "markets": filtered,
            })

        # ── GET /v1/markets/:ticker ──────────────────────────────────────────
        elif path.startswith("/v1/markets/"):
            ticker = path.split("/v1/markets/")[1].upper()
            market_history = sorted(
                [r for r in all_rows if r["ticker"].upper() == ticker],
                key=lambda r: r["timestamp"]
            )
            if not market_history:
                self.send_error_json(f"Ticker '{ticker}' not found", 404)
                return
            latest = market_history[-1]
            self.send_json({
                "ticker": ticker,
                "event": latest["event"],
                "category": latest["category"],
                "current": {
                    "bid": latest["bid"],
                    "ask": latest["ask"],
                    "mid": latest["mid"],
                    "spread": latest["spread"],
                    "spread_pct": latest["spread_pct"],
                    "timestamp": latest["timestamp"],
                },
                "history": [
                    {
                        "timestamp": r["timestamp"],
                        "bid": r["bid"],
                        "ask": r["ask"],
                        "spread_pct": r["spread_pct"],
                    }
                    for r in market_history
                ],
            })

        # ── GET /v1/history ──────────────────────────────────────────────────
        elif path == "/v1/history":
            try:
                days = int(params.get("days", 30))
            except ValueError:
                days = 30
            history = compute_history(all_rows, days)
            category = params.get("category")
            if category:
                history = {k: v for k, v in history.items() if k.lower() == category.lower()}
            self.send_json({
                "days": days,
                "history": history,
            })

        # ── GET /v1/extremes ─────────────────────────────────────────────────
        elif path == "/v1/extremes":
            try:
                n = min(int(params.get("n", 10)), 100)
            except ValueError:
                n = 10
            category = params.get("category")
            markets = snapshot
            if category:
                markets = [m for m in markets if m["category"].lower() == category.lower()]
            sorted_all = sorted(markets, key=lambda m: m["spread_pct"])
            self.send_json({
                "last_updated": max((r["timestamp"] for r in snapshot), default=None),
                "tightest": sorted_all[:n],
                "widest": sorted_all[-n:][::-1],
            })


        # ── GET /v1/efficiency ─────────────────────────────────────────────
        elif path == "/v1/efficiency":
            n = int(params.get("limit", [50])[0])
            cat = params.get("category", [None])[0]
            markets = [r for r in snapshot if not cat or r["category"] == cat]

            for m in markets:
                sprd = m["spread_pct"] or 0.01
                m["efficiency_ratio"] = round(m["volume_24h"] / sprd, 2)
                m["opp_score"] = round(m["spread_pct"] * (m["volume_24h"] ** 0.5), 2)

            with_volume = [m for m in markets if m["volume_24h"] > 0]
            most_efficient = sorted(with_volume, key=lambda x: x["efficiency_ratio"], reverse=True)[:n]
            least_efficient = sorted(with_volume, key=lambda x: x["efficiency_ratio"])[:n]
            top_opportunity = sorted(with_volume, key=lambda x: x["opp_score"], reverse=True)[:n]

            self.send_json({
                "last_updated": snapshot[0]["timestamp"] if snapshot else None,
                "most_efficient": most_efficient,
                "least_efficient": least_efficient,
                "top_opportunity": top_opportunity
            })

        # ── GET /v1/consistency ────────────────────────────────────────────
        elif path == "/v1/consistency":
            from collections import defaultdict
            min_legs = int(params.get("min_legs", [3])[0])
            cat = params.get("category", [None])[0]
            markets = [r for r in snapshot if not cat or r["category"] == cat]

            series_groups = defaultdict(list)
            for m in markets:
                series_groups[m["series"]].append(m)

            results = []
            for series, legs in series_groups.items():
                if len(legs) < min_legs:
                    continue
                bid_sum = sum(m["bid"] for m in legs)
                ask_sum = sum(m["ask"] for m in legs)
                mid_sum = sum(m["mid"] for m in legs)
                avg_spread = round(sum(m["spread_pct"] for m in legs) / len(legs), 2)
                bid_deviation = round(abs(bid_sum - 100), 2)
                ask_deviation = round(abs(ask_sum - 100), 2)
                results.append({
                    "series": series,
                    "event": legs[0]["event"],
                    "category": legs[0]["category"],
                    "n_legs": len(legs),
                    "bid_sum": round(bid_sum, 2),
                    "ask_sum": round(ask_sum, 2),
                    "mid_sum": round(mid_sum, 2),
                    "bid_deviation": bid_deviation,
                    "ask_deviation": ask_deviation,
                    "avg_spread_pct": avg_spread,
                    "legs": legs
                })

            results.sort(key=lambda x: x["bid_deviation"], reverse=True)
            self.send_json({
                "last_updated": snapshot[0]["timestamp"] if snapshot else None,
                "total_events": len(results),
                "events": results[:500]
            })

        # ── 404 ──────────────────────────────────────────────────────────────
        else:
            self.send_error_json(f"Endpoint '{path}' not found. See /v1/status for available endpoints.", 404)

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {self.address_string()} — {args[0]} {args[1]}")

def run_collector():
    """Run collect_spreads.py every 6 hours in background."""
    import time
    collector = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collect_spreads.py")
    while True:
        try:
            subprocess.run([sys.executable, collector], timeout=300)
        except Exception as e:
            print(f"Collector error: {e}")
        time.sleep(6 * 60 * 60)  # 6 hours

if __name__ == "__main__":
    collector_thread = threading.Thread(target=run_collector, daemon=True)
    collector_thread.start()
    print("Background collector started")
    server = http.server.HTTPServer(("0.0.0.0", PORT), APIHandler)
    print(f"╔═══════════════════════════════════════════════════╗")
    print(f"║   Kalshi Spread Intelligence API v1.0.0           ║")
    print(f"║   http://localhost:{PORT}/v1/status                  ║")
    print(f"╚═══════════════════════════════════════════════════╝")
    print(f"\nEndpoints:")
    print(f"  GET /v1/status")
    print(f"  GET /v1/categories")
    print(f"  GET /v1/markets?category=Sports&sort=spread_pct&limit=20")
    print(f"  GET /v1/markets/:ticker")
    print(f"  GET /v1/history?days=7")
    print(f"  GET /v1/extremes?n=10&category=Sports")
    print(f"\nPress Ctrl+C to stop\n")
    server.serve_forever()
