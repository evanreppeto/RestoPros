#!/usr/bin/env python3
"""
yelp_reviews_search.py

For each item on a Monday.com board:

  - Read the item name (company name)
  - Search Yelp: https://www.yelp.com/search?find_desc=<name>&find_loc=Chicago, IL
  - Take the first business result (/biz/ link)
  - Parse "<number> reviews" from that result block
  - Write the number into the "Yelp Reviews" Numbers column

Behavior:
  - Skips items where "Yelp Reviews" already has a non-empty value.
  - If search fails or we cannot parse a count, we log and skip.

Requirements:
    pip install python-dotenv requests beautifulsoup4
"""

import os
import json
import time
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ============== LOAD .env FROM SCRIPT DIR ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")
# ================= CONFIG ==================
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

YELP_LOCATION       = "Chicago, IL"
YELP_SEARCH_URL     = "https://www.yelp.com/search"
YELP_BASE           = "https://www.yelp.com"
YELP_REVIEWS_TITLE  = "Yelp Reviews"  # Numbers column title

MONDAY_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN or "",
    "Content-Type": "application/json"
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 10


# ================= Monday Helpers ==================

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    if not MONDAY_API_TOKEN:
        raise SystemExit("❌ MONDAY_API_TOKEN is empty. Fix your .env file.")

    r = requests.post(
        MONDAY_URL,
        headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    if r.status_code == 401:
        raise SystemExit("❌ 401 Unauthorized from Monday.com. "
                         "Your MONDAY_API_TOKEN is invalid or not set.")
    r.raise_for_status()
    d = r.json()
    if "errors" in d:
        raise RuntimeError(json.dumps(d["errors"], indent=2))
    return d["data"]


def fetch_board(board_id: str) -> Dict[str, Any]:
    q = """
    query($ids:[ID!]){
      boards(ids:$ids){
        name
        columns { id title type settings_str }
      }
    }
    """
    data = gql(q, {"ids":[board_id]})
    boards = data.get("boards") or []
    if not boards:
        raise SystemExit(f"❌ No board returned for id {board_id}. Check MONDAY_BOARD_ID in .env.")
    return boards[0]


def fetch_items(board_id: str) -> List[Dict[str, Any]]:
    q = """
    query($board_id:[ID!], $cursor:String){
      boards(ids:$board_id){
        items_page(limit:500, cursor:$cursor){
          items {
            id
            name
            column_values { id text value type }
          }
          cursor
        }
      }
    }
    """
    items: List[Dict[str, Any]] = []
    cursor = None
    while True:
        d = gql(q, {"board_id":[board_id], "cursor":cursor})
        page = d["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor:
            break
    return items


def col_by_title(columns: List[Dict[str, Any]], title: str) -> Optional[Dict[str, Any]]:
    t = title.strip().lower()
    for c in columns:
        if c["title"].strip().lower() == t:
            return c
    return None


def update_numbers_column(board_id: str, item_id: str, col_id: str, number_value: int) -> None:
    """Monday numbers column wants the value as a string."""
    vals = {col_id: str(number_value)}
    mutation = """
    mutation($b:ID!, $i:ID!, $vals:JSON!){
      change_multiple_column_values(
        board_id:$b,
        item_id:$i,
        column_values:$vals
      ){ id }
    }
    """
    gql(mutation, {"b": board_id, "i": item_id, "vals": json.dumps(vals)})


# ================= Yelp Search Helpers ==================

def fetch_html(url: str, params: Dict[str, str] = None) -> Optional[str]:
    try:
        r = requests.get(
            url,
            headers=REQUEST_HEADERS,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [WARN] fetch_html failed for {url}: {e}")
        return None


def search_yelp_business(company_name: str, location: str) -> Optional[str]:
    """
    Search Yelp for company_name + location.
    Return the URL of the first /biz/ result, or None.
    """
    params = {
        "find_desc": company_name,
        "find_loc": location,
    }
    print(f"    [INFO] Yelp search for: {company_name!r} in {location!r}")
    html = fetch_html(YELP_SEARCH_URL, params=params)
    if not html:
        print("    [WARN] Yelp search HTML not fetched.")
        return None

    soup = BeautifulSoup(html, "lxml")
    # Yelp search results usually have links with href starting with /biz/
    result_link = soup.find("a", href=re.compile(r"^/biz/"))
    if not result_link:
        print("    [INFO] No /biz/ link found on Yelp search results.")
        return None

    biz_href = result_link.get("href", "").strip()
    if not biz_href:
        return None

    biz_url = urljoin(YELP_BASE, biz_href)
    print(f"    [INFO] First Yelp business result: {biz_url}")
    return biz_url


def parse_yelp_review_count(html: str) -> Optional[int]:
    """
    Try to parse a "<number> reviews" pattern from Yelp page text.
    Example: "123 reviews", "1,234 Reviews"
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    m = re.search(r"(\d[\d,]*)\s+reviews", text, re.IGNORECASE)
    if not m:
        return None

    raw = m.group(1)
    try:
        return int(raw.replace(",", ""))
    except ValueError:
        return None


def get_yelp_reviews_by_search(company_name: str, location: str) -> Optional[int]:
    """
    1) Search Yelp for (company_name, location)
    2) Open first /biz/ result
    3) Parse "<num> reviews"
    """
    biz_url = search_yelp_business(company_name, location)
    if not biz_url:
        return None

    html = fetch_html(biz_url)
    if not html:
        print("    [WARN] Could not fetch Yelp business page.")
        return None

    reviews = parse_yelp_review_count(html)
    if reviews is None:
        print("    [INFO] Could not parse review count from Yelp business page.")
    else:
        print(f"    [INFO] Parsed Yelp reviews: {reviews}")
    return reviews


# ================= MAIN ==================

def main():
    if not MONDAY_BOARD_ID:
        raise SystemExit("❌ MONDAY_BOARD_ID is empty. Check your .env file.")

    print("Running Yelp Reviews (search-based) scraper…")
    print(f"[DEBUG] Using .env at: {ENV_PATH}")
    print(f"[DEBUG] MONDAY_BOARD_ID={MONDAY_BOARD_ID}")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]

    yelp_col = col_by_title(cols, YELP_REVIEWS_TITLE)
    if not yelp_col:
        raise SystemExit(f"❌ Column '{YELP_REVIEWS_TITLE}' not found.")

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Yelp Reviews col id = {yelp_col['id']} (type={yelp_col['type']})")

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items fetched: {len(items)}")

    if TARGET_ITEM_ID:
        filtered = [it for it in items if str(it["id"]) == str(TARGET_ITEM_ID)]
        print(f"[INFO] Filtered to TARGET_ITEM_ID={TARGET_ITEM_ID} → {len(filtered)} matching item(s)")
        items = filtered
        if not items:
            print("[INFO] No matching item for TARGET_ITEM_ID; nothing to do.")
            return

    for idx, item in enumerate(items, 1):
        name    = item["name"]
        item_id = item["id"]
        cv_map  = {cv["id"]: cv for cv in item["column_values"]}

        yelp_cv    = cv_map.get(yelp_col["id"])
        current_val = (yelp_cv or {}).get("text") or ""

        print(f"\n[{idx}] {name} (id={item_id})")
        print(f"    Current Yelp Reviews = {current_val!r}")

        # Skip if already has a value
        if current_val.strip():
            print("    → Yelp Reviews already set; skipping.")
            continue

        reviews = get_yelp_reviews_by_search(name, YELP_LOCATION)
        if reviews is None:
            print("    → No review count determined; skipping update.")
            continue

        try:
            update_numbers_column(MONDAY_BOARD_ID, item_id, yelp_col["id"], reviews)
            print("    ✅ Yelp Reviews updated.")
        except Exception as e:
            print(f"    ❌ Update failed: {e}")

        # Be a little polite to Yelp
        time.sleep(1.0)

    print("\nDone ✅")


if __name__ == "__main__":
    main()
