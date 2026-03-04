"""
Vietnam Panic Selling — Flask API Backend

Run:
    pip install flask flask-cors schedule
    python api.py
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import threading
import schedule
import time
import logging
from datetime import datetime

log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

DROPS_FILE = "drops.json"
HISTORY_FILE = "price_history.json"

# ──────────────────────────────────────────────
def load_drops():
    try:
        with open(DROPS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"drops": [], "total_scanned": 0, "total_drops": 0, "scraped_at": None}

def load_history():
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# ──────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/api/drops")
def get_drops():
    """
    GET /api/drops
    Query params:
        city=hanoi|hcm|all
        type=apartment|villa|penthouse|townhouse|all
        min_pct=float     (minimum drop %)
        sort=drop_pct|drop_usd|newest
        limit=int         (default 50)
        page=int          (default 1)
    """
    data = load_drops()
    drops = data.get("drops", [])

    # Filter
    city = request.args.get("city", "all")
    prop_type = request.args.get("type", "all")
    min_pct = float(request.args.get("min_pct", 0))
    sort = request.args.get("sort", "drop_pct")
    limit = int(request.args.get("limit", 50))
    page = int(request.args.get("page", 1))

    if city != "all":
        drops = [d for d in drops if d.get("city") == city]
    if prop_type != "all":
        drops = [d for d in drops if d.get("property_type") == prop_type]
    if min_pct > 0:
        drops = [d for d in drops if (d.get("drop_pct") or 0) >= min_pct]

    # Sort
    if sort == "drop_pct":
        drops.sort(key=lambda x: x.get("drop_pct") or 0, reverse=True)
    elif sort == "drop_usd":
        drops.sort(key=lambda x: x.get("drop_usd") or 0, reverse=True)
    elif sort == "newest":
        drops.sort(key=lambda x: x.get("scraped_at") or "", reverse=True)

    # Paginate
    total = len(drops)
    start = (page - 1) * limit
    end = start + limit
    paged = drops[start:end]

    return jsonify({
        "scraped_at": data.get("scraped_at"),
        "total_scanned": data.get("total_scanned", 0),
        "total_drops": total,
        "page": page,
        "limit": limit,
        "drops": paged,
    })


@app.route("/api/stats")
def get_stats():
    """Summary stats for dashboard header cards"""
    data = load_drops()
    drops = data.get("drops", [])

    hanoi_drops = [d for d in drops if d.get("city") == "hanoi"]
    hcm_drops = [d for d in drops if d.get("city") == "hcm"]

    def calc_stats(lst):
        if not lst:
            return {"count": 0, "avg_pct": 0, "max_pct": 0, "total_usd": 0, "biggest": None}
        pcts = [d.get("drop_pct") or 0 for d in lst]
        usds = [d.get("drop_usd") or 0 for d in lst]
        biggest = max(lst, key=lambda x: x.get("drop_pct") or 0)
        return {
            "count": len(lst),
            "avg_pct": round(sum(pcts) / len(pcts), 1),
            "max_pct": round(max(pcts), 1),
            "total_usd": round(sum(usds)),
            "biggest": {
                "title": biggest.get("title"),
                "city": biggest.get("city"),
                "district": biggest.get("district"),
                "drop_pct": biggest.get("drop_pct"),
                "drop_usd": biggest.get("drop_usd"),
                "current_price_usd": biggest.get("current_price_usd"),
                "prev_price_usd": biggest.get("prev_price_usd"),
            }
        }

    return jsonify({
        "scraped_at": data.get("scraped_at"),
        "total_scanned": data.get("total_scanned", 0),
        "all": calc_stats(drops),
        "hanoi": calc_stats(hanoi_drops),
        "hcm": calc_stats(hcm_drops),
    })


@app.route("/api/listing/<listing_id>")
def get_listing(listing_id):
    """Get full price history for a specific listing"""
    history = load_history()
    if listing_id not in history:
        return jsonify({"error": "Listing not found"}), 404
    return jsonify(history[listing_id])


# ──────────────────────────────────────────────
# SCHEDULER (auto-rescrape every 15 min)
# ──────────────────────────────────────────────
def run_scraper():
    import sys
    sys.path.insert(0, "../scraper")
    try:
        from scraper import run_scrape
        log.info("Running scheduled scrape...")
        run_scrape()
    except Exception as e:
        log.error("Scrape failed: %s", e)

def scheduler_thread():
    schedule.every(15).minutes.do(run_scraper)
    while True:
        schedule.run_pending()
        time.sleep(60)

# Start scheduler in background (runs under gunicorn too)
t = threading.Thread(target=scheduler_thread, daemon=True)
t.start()
run_scraper()  # run immediately on startup

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
    log.info("Scheduler started — scraping every 15 minutes")

    app.run(host="0.0.0.0", port=5000, debug=False)
