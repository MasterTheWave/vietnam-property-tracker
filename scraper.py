"""
Vietnam Real Estate Price Tracker — Scraper
Uses nhatot.com (chotot.com) public API

Region codes:
  13000 = Ho Chi Minh City
  12000 = Hanoi

Category codes:
  1020 = Nha o (houses/apartments)
"""

import requests
import json
import time
import logging
import hashlib
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nhatot.com/",
}

# API config
API_BASE = "https://gateway.chotot.com/v1/public/ad-listing"
REGIONS = {
    "hcm":   13000,
    "hanoi": 12000,
}
CATEGORY = 1020  # residential property
PAGE_SIZE = 50

USD_VND_RATE = 25400

# ──────────────────────────────────────────────
# EXCHANGE RATE
# ──────────────────────────────────────────────
def get_usd_rate() -> float:
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        return r.json()["rates"]["VND"]
    except Exception:
        log.warning("Using fallback USD/VND rate: %s", USD_VND_RATE)
        return USD_VND_RATE

# ──────────────────────────────────────────────
# DATA MODEL
# ──────────────────────────────────────────────
@dataclass
class Listing:
    id: str
    source: str
    url: str
    title: str
    city: str
    district: str
    property_type: str
    size_m2: Optional[float]
    current_price_vnd: float
    current_price_usd: float
    prev_price_vnd: Optional[float]
    prev_price_usd: Optional[float]
    drop_vnd: Optional[float]
    drop_usd: Optional[float]
    drop_pct: Optional[float]
    scraped_at: str
    first_seen: str

    def to_dict(self):
        return asdict(self)

# ──────────────────────────────────────────────
# PRICE HISTORY
# ──────────────────────────────────────────────
HISTORY_FILE = "price_history.json"

def load_history() -> dict:
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_history(history: dict):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def detect_price_change(listing_id, current_price, history):
    if listing_id not in history:
        return None, None, None, None
    prev = history[listing_id]["price"]
    if prev <= 0:
        return None, None, None, None
    if current_price < prev * 0.98:
        change = prev - current_price
        pct = (change / prev) * 100
        return prev, change, round(pct, 1), "down"
    elif current_price > prev * 1.02:
        change = current_price - prev
        pct = (change / prev) * 100
        return prev, change, round(pct, 1), "up"
    return None, None, None, None

# ──────────────────────────────────────────────
# NHATOT API SCRAPER
# ──────────────────────────────────────────────
def get_property_type(ad) -> str:
    house_type = ad.get("house_type", 0)
    type_map = {
        1: "apartment",
        2: "apartment",
        3: "villa",
        4: "townhouse",
        5: "townhouse",
    }
    return type_map.get(house_type, "apartment")

def fetch_listings(city_key: str, usd_rate: float, pages: int = 5) -> list:
    region = REGIONS[city_key]
    listings = []
    session = requests.Session()

    for page in range(pages):
        offset = page * PAGE_SIZE
        url = f"{API_BASE}?region_v2={region}&cg={CATEGORY}&st=s,k&limit={PAGE_SIZE}&o={offset}"
        log.info("Fetching %s page %d: %s", city_key, page + 1, url)

        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("API request failed: %s", e)
            time.sleep(3)
            continue

        ads = data.get("ads", [])
        log.info("Got %d listings", len(ads))

        if not ads:
            break

        for ad in ads:
            try:
                price_vnd = ad.get("price", 0)
                if not price_vnd or price_vnd < 100_000_000:
                    continue

                ad_id = str(ad.get("ad_id", ""))
                listing_id = hashlib.md5(ad_id.encode()).hexdigest()[:12]

                title = ad.get("subject", "") or ad.get("body", "")[:80]
                district = ad.get("street_name", "") or ad.get("location", "")
                size_m2 = ad.get("size")
                prop_type = get_property_type(ad)
                now = datetime.utcnow().isoformat()
                url_listing = f"https://www.nhatot.com/{ad_id}.htm"

                listing = Listing(
                    id=listing_id,
                    source="nhatot.com",
                    url=url_listing,
                    title=title,
                    city=city_key,
                    district=district,
                    property_type=prop_type,
                    size_m2=float(size_m2) if size_m2 else None,
                    current_price_vnd=float(price_vnd),
                    current_price_usd=round(float(price_vnd) / usd_rate),
                    prev_price_vnd=None,
                    prev_price_usd=None,
                    drop_vnd=None,
                    drop_usd=None,
                    drop_pct=None,
                    scraped_at=now,
                    first_seen=now,
                )
                listings.append(listing)
            except Exception as e:
                log.debug("Parse error: %s", e)

        time.sleep(1)

    return listings

# ──────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────
def run_scrape():
    log.info("=" * 60)
    log.info("Vietnam Property Tracker — Scraper Starting")
    log.info("=" * 60)

    usd_rate = get_usd_rate()
    log.info("USD/VND rate: %s", usd_rate)

    history = load_history()
    all_listings = []
    changes = []

    for city_key in REGIONS:
        log.info("→ Scraping %s", city_key)
        results = fetch_listings(city_key, usd_rate, pages=5)
        all_listings.extend(results)
        log.info("  Total from %s: %d", city_key, len(results))

    log.info("Total listings scraped: %d", len(all_listings))

    # Detect price changes
    for listing in all_listings:
        prev, change_amt, change_pct, direction = detect_price_change(
            listing.id, listing.current_price_vnd, history
        )
        if prev and change_pct:
            listing.prev_price_vnd = prev
            listing.prev_price_usd = round(prev / usd_rate)
            listing.drop_vnd = change_amt
            listing.drop_usd = round(change_amt / usd_rate)
            listing.drop_pct = change_pct
            changes.append(listing)
            symbol = "↓" if direction == "down" else "↑"
            log.info("  PRICE CHANGE: %s — %.1f%% %s", listing.title[:50], change_pct, symbol)

        # Update history
        if listing.id not in history or listing.current_price_vnd != history[listing.id]["price"]:
            history[listing.id] = {
                "price": listing.current_price_vnd,
                "title": listing.title,
                "city": listing.city,
                "first_seen": history.get(listing.id, {}).get("first_seen", listing.first_seen),
                "updated_at": listing.scraped_at,
            }

    save_history(history)
    log.info("History saved. Total tracked: %d", len(history))

    changes.sort(key=lambda x: x.drop_pct or 0, reverse=True)

    # First run — show all listings since no price history yet
    output_drops = changes if changes else all_listings[:50]

    output = {
        "scraped_at": datetime.utcnow().isoformat(),
        "usd_vnd_rate": usd_rate,
        "total_scanned": len(all_listings),
        "total_drops": len(changes),
        "drops": [d.to_dict() for d in output_drops],
    }

    with open("drops.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Saved to drops.json — %d price changes found", len(changes))
    return output

if __name__ == "__main__":
    run_scrape()
