"""
Vietnam Real Estate Price Tracker — Scraper
Targets: homedy.com

Usage:
    pip install requests beautifulsoup4
    python scraper.py
"""

import requests
import json
import time
import logging
import hashlib
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://homedy.com/",
}

# homedy.com URL patterns (verified working)
SEARCH_URLS = {
    "hanoi_apartment":  "https://homedy.com/ban-can-ho-ha-noi",
    "hanoi_villa":      "https://homedy.com/ban-biet-thu-ha-noi",
    "hanoi_townhouse":  "https://homedy.com/ban-nha-mat-pho-ha-noi",
    "hcm_apartment":    "https://homedy.com/ban-can-ho-ho-chi-minh",
    "hcm_villa":        "https://homedy.com/ban-biet-thu-ho-chi-minh",
    "hcm_townhouse":    "https://homedy.com/ban-nha-mat-pho-ho-chi-minh",
}

CITY_MAP = {
    "hanoi_apartment": "hanoi", "hanoi_villa": "hanoi", "hanoi_townhouse": "hanoi",
    "hcm_apartment":   "hcm",   "hcm_villa":   "hcm",   "hcm_townhouse":   "hcm",
}

TYPE_MAP = {
    "hanoi_apartment": "apartment", "hanoi_villa": "villa", "hanoi_townhouse": "townhouse",
    "hcm_apartment":   "apartment", "hcm_villa":   "villa", "hcm_townhouse":   "townhouse",
}

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
# PRICE PARSER
# ──────────────────────────────────────────────
def parse_price_vnd(text: str) -> Optional[float]:
    try:
        text = text.lower().strip().replace(",", ".").replace(" ", "")
        if "tỷ" in text or "ty" in text:
            val = float(text.replace("tỷ", "").replace("ty", "").strip())
            return val * 1_000_000_000
        elif "triệu" in text or "trieu" in text:
            val = float(text.replace("triệu", "").replace("trieu", "").strip())
            return val * 1_000_000
        elif "thỏa thuận" in text or "thoathuan" in text:
            return None  # negotiable price, skip
        return None
    except Exception:
        return None

# ──────────────────────────────────────────────
# HOMEDY SCRAPER
# ──────────────────────────────────────────────
class HomedyScraper:
    BASE_URL = "https://homedy.com"

    def __init__(self, session: requests.Session, usd_rate: float):
        self.session = session
        self.usd_rate = usd_rate

    def scrape(self, key: str, pages: int = 3) -> list:
        listings = []
        base_url = SEARCH_URLS[key]
        city = CITY_MAP[key]
        prop_type = TYPE_MAP[key]

        for page in range(1, pages + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            log.info("Homedy scraping: %s", url)

            try:
                r = self.session.get(url, headers=HEADERS, timeout=15)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                log.error("Failed: %s — %s", url, e)
                time.sleep(3)
                continue

            items = soup.select("li.product-item")
            log.info("Found %d items on page %d", len(items), page)

            for item in items:
                try:
                    listing = self._parse_item(item, city, prop_type)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.debug("Parse error: %s", e)

            time.sleep(2)

        return listings

    def _parse_item(self, item, city: str, prop_type: str) -> Optional[Listing]:
        # Title + URL
        title_el = item.select_one("h3 a.title") or item.select_one("a.title") or item.select_one("h3 a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = self.BASE_URL + href if href.startswith("/") else href
        if not url or not title:
            return None

        # Price
        price_el = item.select_one(".price") or item.select_one("[class*='price']")
        if not price_el:
            return None
        price_vnd = parse_price_vnd(price_el.get_text(strip=True))
        if not price_vnd:
            return None

        # District/location
        loc_el = item.select_one(".location") or item.select_one("[class*='location']") or item.select_one(".address")
        district = loc_el.get_text(strip=True) if loc_el else ""

        # Size
        size_m2 = None
        size_el = item.select_one(".area") or item.select_one("[class*='area']")
        if size_el:
            try:
                size_text = size_el.get_text(strip=True).replace("m²","").replace("m2","").strip()
                size_m2 = float(''.join(c for c in size_text if c.isdigit() or c == '.'))
            except Exception:
                pass

        listing_id = hashlib.md5(url.encode()).hexdigest()[:12]
        now = datetime.utcnow().isoformat()

        return Listing(
            id=listing_id,
            source="homedy.com",
            url=url,
            title=title,
            city=city,
            district=district,
            property_type=prop_type,
            size_m2=size_m2,
            current_price_vnd=price_vnd,
            current_price_usd=round(price_vnd / self.usd_rate),
            prev_price_vnd=None,
            prev_price_usd=None,
            drop_vnd=None,
            drop_usd=None,
            drop_pct=None,
            scraped_at=now,
            first_seen=now,
        )

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
    session = requests.Session()
    scraper = HomedyScraper(session, usd_rate)

    all_listings = []
    changes = []

    for key in SEARCH_URLS:
        log.info("→ Scraping %s", key)
        results = scraper.scrape(key, pages=3)
        all_listings.extend(results)
        log.info("  Got %d listings", len(results))

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
