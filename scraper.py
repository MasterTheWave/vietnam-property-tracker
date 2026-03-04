"""
Vietnam Real Estate Price Drop Scraper
Targets: batdongsan.com.vn, mogi.vn, nhadat24h.net

Usage:
    pip install requests beautifulsoup4 selenium playwright
    python scraper.py
"""

import requests
import json
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional
from bs4 import BeautifulSoup
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

CITIES = {
    "hanoi": {"name": "Hà Nội", "code": "ha-noi"},
    "hcm":   {"name": "Hồ Chí Minh", "code": "tp-ho-chi-minh"},
}

PROPERTY_TYPES = {
    "apartment": "can-ho-chung-cu",
    "villa":     "biet-thu-lien-ke",
    "penthouse": "can-ho-penthouse",
    "townhouse": "nha-mat-pho",
}

# ──────────────────────────────────────────────
# DATA MODEL
# ──────────────────────────────────────────────
@dataclass
class Listing:
    id: str
    source: str          # batdongsan / mogi / nhadat24h
    url: str
    title: str
    city: str            # hanoi / hcm
    district: str
    property_type: str
    size_m2: Optional[float]
    bedrooms: Optional[int]
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
# EXCHANGE RATE (fetch live or use fallback)
# ──────────────────────────────────────────────
USD_VND_RATE = 25_400  # fallback rate

def get_usd_rate() -> float:
    try:
        r = requests.get(
            "https://api.exchangerate-api.com/v4/latest/USD",
            timeout=5
        )
        return r.json()["rates"]["VND"]
    except Exception:
        log.warning("Could not fetch exchange rate, using fallback: %s", USD_VND_RATE)
        return USD_VND_RATE


# ──────────────────────────────────────────────
# PRICE HISTORY STORE (simple JSON file)
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

def detect_drop(listing_id: str, current_price: float, history: dict) -> tuple:
    """
    Returns (prev_price, drop_amt, drop_pct) or (None, None, None)
    """
    if listing_id not in history:
        return None, None, None

    prev = history[listing_id]["price"]
    if current_price < prev:
        drop = prev - current_price
        pct = (drop / prev) * 100
        return prev, drop, pct
    return None, None, None


# ──────────────────────────────────────────────
# SCRAPER: batdongsan.com.vn
# ──────────────────────────────────────────────
class BatDongSanScraper:
    BASE_URL = "https://batdongsan.com.vn"

    def __init__(self, session: requests.Session, usd_rate: float):
        self.session = session
        self.usd_rate = usd_rate

    def scrape_city(self, city_code: str, city_key: str, prop_type: str, pages: int = 5) -> list[Listing]:
        listings = []
        url_type = PROPERTY_TYPES.get(prop_type, "can-ho-chung-cu")

        for page in range(1, pages + 1):
            url = f"{self.BASE_URL}/{url_type}/{city_code}/p{page}"
            log.info("BatDongSan scraping: %s", url)

            try:
                r = self.session.get(url, headers=HEADERS, timeout=15)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                log.error("Failed: %s — %s", url, e)
                time.sleep(5)
                continue

            items = soup.select("div.js__card")
            if not items:
                items = soup.select("div[class*='re__card-full']")

            for item in items:
                try:
                    listing = self._parse_item(item, city_key, prop_type)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.debug("Parse error: %s", e)

            time.sleep(2)  # polite delay

        return listings

    def _parse_item(self, item, city_key: str, prop_type: str) -> Optional[Listing]:
        # Title
        title_el = item.select_one("span.js__card-title") or item.select_one("h3")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        # URL
        link_el = item.select_one("a[href]")
        url = self.BASE_URL + link_el["href"] if link_el and link_el["href"].startswith("/") else (link_el["href"] if link_el else "")

        # Price — batdongsan shows in billions VND (tỷ) or millions (triệu)
        price_el = item.select_one("span.re__card-config-price")
        if not price_el:
            return None
        price_text = price_el.get_text(strip=True).lower()
        price_vnd = self._parse_price_vnd(price_text)
        if not price_vnd:
            return None

        # District
        loc_el = item.select_one("div.re__card-location") or item.select_one("span[class*='location']")
        district = loc_el.get_text(strip=True) if loc_el else ""

        # Size
        size_el = item.select_one("span.re__card-config-area") or item.select_one("span[class*='area']")
        size_m2 = None
        if size_el:
            try:
                size_m2 = float(size_el.get_text(strip=True).replace("m²","").replace(",","").strip())
            except:
                pass

        # Unique ID
        listing_id = hashlib.md5(url.encode()).hexdigest()[:12]
        now = datetime.utcnow().isoformat()

        return Listing(
            id=listing_id,
            source="batdongsan.com.vn",
            url=url,
            title=title,
            city=city_key,
            district=district,
            property_type=prop_type,
            size_m2=size_m2,
            bedrooms=None,
            current_price_vnd=price_vnd,
            current_price_usd=price_vnd / self.usd_rate,
            prev_price_vnd=None,
            prev_price_usd=None,
            drop_vnd=None,
            drop_usd=None,
            drop_pct=None,
            scraped_at=now,
            first_seen=now,
        )

    def _parse_price_vnd(self, text: str) -> Optional[float]:
        """Convert Vietnamese price string to VND"""
        try:
            text = text.replace(",", ".").strip()
            if "tỷ" in text:
                val = float(text.replace("tỷ", "").strip())
                return val * 1_000_000_000
            elif "triệu" in text:
                val = float(text.replace("triệu", "").strip())
                return val * 1_000_000
            return None
        except:
            return None


# ──────────────────────────────────────────────
# SCRAPER: mogi.vn
# ──────────────────────────────────────────────
class MogiScraper:
    BASE_URL = "https://mogi.vn"
    CITY_MAP = {"hanoi": "ha-noi", "hcm": "ho-chi-minh"}
    TYPE_MAP = {
        "apartment": "can-ho-chung-cu",
        "villa":     "biet-thu",
        "penthouse": "can-ho-chung-cu",
        "townhouse": "nha-pho",
    }

    def __init__(self, session: requests.Session, usd_rate: float):
        self.session = session
        self.usd_rate = usd_rate

    def scrape_city(self, city_key: str, prop_type: str, pages: int = 3) -> list[Listing]:
        listings = []
        city_slug = self.CITY_MAP[city_key]
        type_slug = self.TYPE_MAP.get(prop_type, "can-ho-chung-cu")

        for page in range(1, pages + 1):
            url = f"{self.BASE_URL}/{city_slug}/ban-{type_slug}?page={page}"
            log.info("Mogi scraping: %s", url)

            try:
                r = self.session.get(url, headers=HEADERS, timeout=15)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                log.error("Failed: %s — %s", url, e)
                time.sleep(5)
                continue

            items = soup.select("ul.prop-list > li")

            for item in items:
                try:
                    listing = self._parse_item(item, city_key, prop_type)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.debug("Parse error: %s", e)

            time.sleep(2)

        return listings

    def _parse_item(self, item, city_key: str, prop_type: str) -> Optional[Listing]:
        title_el = item.select_one("h2.prop-name a") or item.select_one("a.link-title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        url = title_el.get("href", "")
        if url and not url.startswith("http"):
            url = self.BASE_URL + url

        price_el = item.select_one("strong.price") or item.select_one("span[class*='price']")
        if not price_el:
            return None
        price_vnd = self._parse_price(price_el.get_text(strip=True))
        if not price_vnd:
            return None

        loc_el = item.select_one("span.address") or item.select_one("p.prop-addr")
        district = loc_el.get_text(strip=True) if loc_el else ""

        listing_id = hashlib.md5(url.encode()).hexdigest()[:12]
        now = datetime.utcnow().isoformat()

        return Listing(
            id=listing_id,
            source="mogi.vn",
            url=url,
            title=title,
            city=city_key,
            district=district,
            property_type=prop_type,
            size_m2=None,
            bedrooms=None,
            current_price_vnd=price_vnd,
            current_price_usd=price_vnd / self.usd_rate,
            prev_price_vnd=None,
            prev_price_usd=None,
            drop_vnd=None,
            drop_usd=None,
            drop_pct=None,
            scraped_at=now,
            first_seen=now,
        )

    def _parse_price(self, text: str) -> Optional[float]:
        try:
            text = text.lower().replace(",", ".").strip()
            if "tỷ" in text:
                val = float(text.replace("tỷ", "").strip())
                return val * 1_000_000_000
            elif "triệu" in text:
                val = float(text.replace("triệu", "").strip())
                return val * 1_000_000
            return None
        except:
            return None


# ──────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────
def run_scrape():
    log.info("=" * 60)
    log.info("Vietnam Real Estate Panic Selling Scraper")
    log.info("=" * 60)

    usd_rate = get_usd_rate()
    log.info("USD/VND rate: %s", usd_rate)

    history = load_history()
    session = requests.Session()

    bds_scraper = BatDongSanScraper(session, usd_rate)
    mogi_scraper = MogiScraper(session, usd_rate)

    all_listings: list[Listing] = []
    drops: list[Listing] = []

    # Scrape each city × property type
    for city_key, city_info in CITIES.items():
        for prop_type in PROPERTY_TYPES:
            log.info("→ Scraping %s / %s", city_info["name"], prop_type)

            # BatDongSan
            bds_results = bds_scraper.scrape_city(city_info["code"], city_key, prop_type, pages=5)
            all_listings.extend(bds_results)

            # Mogi
            mogi_results = mogi_scraper.scrape_city(city_key, prop_type, pages=3)
            all_listings.extend(mogi_results)

    log.info("Total listings scraped: %d", len(all_listings))

    # Detect price drops
    for listing in all_listings:
        prev, drop_vnd, drop_pct = detect_drop(listing.id, listing.current_price_vnd, history)

        if prev and drop_pct and drop_pct >= 2.0:  # Only flag 2%+ drops
            listing.prev_price_vnd = prev
            listing.prev_price_usd = prev / usd_rate
            listing.drop_vnd = drop_vnd
            listing.drop_usd = drop_vnd / usd_rate
            listing.drop_pct = drop_pct
            drops.append(listing)
            log.info("  DROP DETECTED: %s — %.1f%% (-$%.0f USD)", listing.title[:50], drop_pct, listing.drop_usd)

        # Update history
        if listing.id not in history or listing.current_price_vnd < history[listing.id]["price"]:
            history[listing.id] = {
                "price": listing.current_price_vnd,
                "title": listing.title,
                "city": listing.city,
                "first_seen": history.get(listing.id, {}).get("first_seen", listing.first_seen),
                "updated_at": listing.scraped_at,
            }

    save_history(history)
    log.info("Price history saved. Total tracked: %d", len(history))

    # Sort drops by % descending
    drops.sort(key=lambda x: x.drop_pct or 0, reverse=True)

    # Save results
    output = {
        "scraped_at": datetime.utcnow().isoformat(),
        "usd_vnd_rate": usd_rate,
        "total_scanned": len(all_listings),
        "total_drops": len(drops),
        "drops": [d.to_dict() for d in drops],
    }

    with open("drops.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Drops saved to drops.json — %d drops found", len(drops))
    return output


if __name__ == "__main__":
    run_scrape()
