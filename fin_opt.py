#!/usr/bin/env python3
"""
financing_options.py

Scans each company's website to detect financing options.
Writes the detected text into the Monday.com text column: "Financing Options"

Requirements:
    pip install python-dotenv requests beautifulsoup4 tldextract
"""

import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# ========= CONFIG =========
load_dotenv()
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE    = "Website"
FINANCING_COL_TITLE  = "Financing Options"   # <-- TEXT COLUMN

MONDAY_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN or "",
    "Content-Type": "application/json"
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
}
REQUEST_TIMEOUT = 12

MAX_EXTRA_PAGES = 5   # scan 5 internal financing-related pages

# Keywords strongly indicating financing options
FINANCING_KEYWORDS = [
    "financing", "finance options", "payment plans", "low monthly payment",
    "0% apr", "zero interest", "apply for financing", "apply for credit",
    "special financing", "loan", "credit approval", "synchrony", "affirm",
    "klarna", "get approved", "finance available"
]
# ================================================================


# ---------- Monday API Helpers ----------

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(MONDAY_URL, headers=MONDAY_HEADERS,
                      json={"query": query, "variables": variables or {}},
                      timeout=30)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]


def fetch_board(board_id: str):
    q = """
    query($ids:[ID!]){
      boards(ids:$ids){
        name
        columns { id title type settings_str }
      }
    }
    """
    return gql(q, {"ids":[board_id]})["boards"][0]


def fetch_items(board_id: str) -> List[Dict[str,Any]]:
    q = """
    query($board_id:[ID!], $cursor:String){
      boards(ids:$board_id){
        items_page(limit:500, cursor:$cursor){
          items{
            id
            name
            column_values { id text value }
          }
          cursor
        }
      }
    }
    """
    items = []
    cursor = None

    while True:
        d = gql(q, {"board_id":[board_id], "cursor":cursor})
        page = d["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor:
            break

    return items


def col_by_title(cols: List[Dict[str,Any]], title: str):
    for c in cols:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None


def update_text_column(board_id: str, item_id: str, col_id: str, text_value: str):
    vals = {col_id: text_value}
    m = """
    mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id,
                                    item_id:$item_id,
                                    column_values:$vals){
        id
      }
    }
    """
    gql(m, {"board_id":board_id,
            "item_id": item_id,
            "vals": json.dumps(vals)})


def extract_website(column_value: Dict[str,Any], col_type: str) -> str:
    if not column_value:
        return ""

    if col_type.lower() == "link":
        # Try JSON value.url
        raw = column_value.get("value")
        if raw:
            try:
                j = json.loads(raw)
                if j.get("url"):
                    return j["url"]
            except:
                pass
        return column_value.get("text") or ""

    return column_value.get("text") or ""


# ---------- Web Scraping ----------

def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    if not url.startswith(("http://","https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        return url if parsed.netloc else None
    except:
        return None


def fetch_page_text(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        return soup.get_text(" ", strip=True)[:50000]
    except:
        return None


def find_internal_financing_pages(base_url: str, home_html: str) -> List[str]:
    soup = BeautifulSoup(home_html, "lxml")
    parsed = urlparse(base_url)
    base_netloc = parsed.netloc.lower()

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        try:
            if urlparse(abs_url).netloc.lower() != base_netloc:
                continue
        except:
            continue

        # Look for financing-related URL paths
        if any(word in abs_url.lower() for word in ["finance","financ","payment","payments","apply","credit"]):
            candidates.append(abs_url)

    # Deduplicate
    uniq = []
    seen = set()
    for u in candidates:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
        if len(uniq) >= MAX_EXTRA_PAGES:
            break

    return uniq


def detect_financing(url: str) -> str:
    """
    Returns financing text or "None"
    """
    norm = normalize_url(url)
    if not norm:
        return "None"

    # Fetch homepage
    homepage = fetch_page_text(norm)
    if not homepage:
        return "None"

    # Search homepage
    low = homepage.lower()
    for kw in FINANCING_KEYWORDS:
        if kw in low:
            return "Financing available"

    # Search additional pages
    internal = find_internal_financing_pages(norm, homepage)
    for page_url in internal:
        sub = fetch_page_text(page_url)
        if not sub:
            continue
        low = sub.lower()
        for kw in FINANCING_KEYWORDS:
            if kw in low:
                return "Financing available"

    return "None"


# ---------- MAIN ----------

def main():
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        raise SystemExit("❌ Missing MONDAY_API_TOKEN or MONDAY_BOARD_ID in .env")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]

    website_col   = col_by_title(cols, WEBSITE_COL_TITLE)
    financing_col = col_by_title(cols, FINANCING_COL_TITLE)

    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")
    if not financing_col:
        raise SystemExit(f"❌ Column '{FINANCING_COL_TITLE}' not found.")

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Website column       = {website_col['id']}")
    print(f"[INFO] Financing Options col = {financing_col['id']}")
    print()

    items = fetch_items(MONDAY_BOARD_ID)

    if TARGET_ITEM_ID:
        filtered = [it for it in items if str(it["id"]) == str(TARGET_ITEM_ID)]
        print(f"[INFO] Filtered to TARGET_ITEM_ID={TARGET_ITEM_ID} → {len(filtered)} matching item(s)")
        items = filtered
        if not items:
            print("[INFO] No matching item for TARGET_ITEM_ID; nothing to do.")
            return

    for i, item in enumerate(items, 1):
        name = item["name"]
        cv_map = {c["id"]: c for c in item["column_values"]}

        current_val = cv_map.get(financing_col["id"], {}).get("text") or ""
        website_val = extract_website(cv_map.get(website_col["id"]), website_col["type"])

        print(f"[{i}] {name}")
        print(f"     Website: {website_val!r}")

        financing = detect_financing(website_val)
        print(f"     Detected: {financing}")

        # Update Monday only if changed
        if current_val.strip() != financing:
            try:
                update_text_column(MONDAY_BOARD_ID, item["id"], financing_col["id"], financing)
                print("     ✅ Updated!")
            except Exception as e:
                print(f"     ❌ Update failed: {e}")
        else:
            print("     (unchanged)")

        print()
        time.sleep(1.0)

    print("Done ✅")


if __name__ == "__main__":
    main()