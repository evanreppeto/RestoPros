#!/usr/bin/env python3
"""
followers_count.py

For each item on a Monday.com board:

  - Uses the Website (Link) column to fetch the company site
  - Extracts links to Facebook / Instagram / TikTok / LinkedIn
  - Scrapes each public social profile page (no login) and tries to parse
    follower counts using simple regex heuristics
  - Sums all followers found across platforms
  - Writes the sum into the Numbers column "Followers Count"

Behavior:
  - Skips items where "Followers Count" already has a value
  - If no followers can be determined -> leaves the cell untouched (no update)

Requirements:
    pip install python-dotenv requests beautifulsoup4
"""

import os
import json
import time
import re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ============== LOAD .env FROM SCRIPT DIR ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)



# ================= CONFIG ==================
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE   = "Website"          # Link column title
FOLLOWERS_COL_TITLE = "Followers Count"  # Numbers column title

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

SOCIAL_KEYWORDS = {
    "facebook": "facebook.com",
    "instagram": "instagram.com",
    "tiktok": "tiktok.com",
    "linkedin": "linkedin.com",
}

# ================= Monday Helpers ==================

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    if not MONDAY_API_TOKEN:
        raise SystemExit("❌ MONDAY_API_TOKEN is empty. Check your .env file.")
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


def update_number_column(board_id: str, item_id: str, col_id: str, number_value: float) -> None:
    vals = {col_id: str(number_value)}
    m = """
    mutation($b:ID!, $i:ID!, $vals:JSON!){
      change_multiple_column_values(
        board_id:$b,
        item_id:$i,
        column_values:$vals
      ){ id }
    }
    """
    gql(m, {"b": board_id, "i": item_id, "vals": json.dumps(vals)})


def extract_website_from_link_cv(cv: Dict[str, Any]) -> Optional[str]:
    """Get the actual URL from a Monday Link column."""
    if not cv:
        return None
    raw_val = cv.get("value")
    if raw_val:
        try:
            j = json.loads(raw_val)
            url = (j.get("url") or "").strip()
            if url:
                return url
        except Exception:
            pass
    return (cv.get("text") or "").strip() or None


# ================= HTTP / Parsing Helpers ==================

def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [WARN] fetch_html failed for {url}: {e}")
        return None


def normalize_base_url(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    try:
        parsed = urlparse(u)
        if not parsed.netloc:
            return None
        # remove query/fragment
        clean = parsed._replace(query="", fragment="").geturl()
        return clean
    except Exception:
        return None


def find_social_links(website_url: str, html: str) -> Dict[str, str]:
    """
    Scan the website HTML and return the first social link per platform:
      { "facebook": "https://facebook.com/...", "instagram": "...", ... }
    """
    soup = BeautifulSoup(html, "lxml")
    social_links: Dict[str, str] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue

        abs_url = urljoin(website_url, href)
        low = abs_url.lower()

        for platform, needle in SOCIAL_KEYWORDS.items():
            if needle in low and platform not in social_links:
                social_links[platform] = abs_url

    return social_links


def parse_number_token(raw: str) -> Optional[int]:
    """
    Convert strings like:
        '12.3K' -> 12300
        '120K'  -> 120000
        '1.2M'  -> 1200000
        '123,456' -> 123456
    into an integer.
    """
    raw = raw.strip()
    if not raw:
        return None

    multiplier = 1.0
    last = raw[-1].lower()
    if last == "k":
        multiplier = 1_000.0
        raw = raw[:-1]
    elif last == "m":
        multiplier = 1_000_000.0
        raw = raw[:-1]

    raw_clean = raw.replace(",", "").strip()
    try:
        base = float(raw_clean)
    except ValueError:
        return None

    return int(round(base * multiplier))


# ================= Platform-specific follower scrapers ==================

def get_instagram_followers(url: str) -> Optional[int]:
    html = fetch_html(url)
    if not html:
        return None

    # Try JSON fragment: "edge_followed_by":{"count":12345}
    m = re.search(r'"edge_followed_by"\s*:\s*{\s*"count"\s*:\s*(\d+)', html)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass

    # Fallback: look for '123K followers' text
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m2 = re.search(r"(\d[\d,\.]*\s*[KkMm]?)\s+followers", text, re.IGNORECASE)
    if m2:
        return parse_number_token(m2.group(1))

    return None


def get_tiktok_followers(url: str) -> Optional[int]:
    html = fetch_html(url)
    if not html:
        return None

    # TikTok often has "followerCount":12345 in JSON
    m = re.search(r'"followerCount"\s*:\s*(\d+)', html)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass

    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m2 = re.search(r"(\d[\d,\.]*\s*[KkMm]?)\s+Followers", text, re.IGNORECASE)
    if m2:
        return parse_number_token(m2.group(1))

    return None


def get_facebook_followers(url: str) -> Optional[int]:
    html = fetch_html(url)
    if not html:
        return None

    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"(\d[\d,\.]*\s*[KkMm]?)\s+followers", text, re.IGNORECASE)
    if m:
        return parse_number_token(m.group(1))

    return None


def get_linkedin_followers(url: str) -> Optional[int]:
    """
    LinkedIn is often login-walled; this will only work for some public pages.
    """
    html = fetch_html(url)
    if not html:
        return None

    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"(\d[\d,\.]*\s*[KkMm]?)\s+followers", text, re.IGNORECASE)
    if m:
        return parse_number_token(m.group(1))

    return None


def get_followers_for_platform(platform: str, url: str) -> Optional[int]:
    platform = platform.lower()
    if platform == "instagram":
        return get_instagram_followers(url)
    if platform == "tiktok":
        return get_tiktok_followers(url)
    if platform == "facebook":
        return get_facebook_followers(url)
    if platform == "linkedin":
        return get_linkedin_followers(url)
    return None


# ================= MAIN ==================

def main():
    if not MONDAY_BOARD_ID:
        raise SystemExit("❌ MONDAY_BOARD_ID is empty. Check your .env file.")

    print("Running Followers Count scraper…")
    print(f"[DEBUG] Using .env at: {ENV_PATH}")
    print(f"[DEBUG] MONDAY_BOARD_ID={MONDAY_BOARD_ID}")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]

    website_col   = col_by_title(cols, WEBSITE_COL_TITLE)
    followers_col = col_by_title(cols, FOLLOWERS_COL_TITLE)

    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")
    if not followers_col:
        raise SystemExit(f"❌ Column '{FOLLOWERS_COL_TITLE}' not found.")

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Website col id        = {website_col['id']} (type={website_col['type']})")
    print(f"[INFO] Followers Count col   = {followers_col['id']} (type={followers_col['type']})")

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items fetched: {len(items)}")

    for idx, item in enumerate(items, 1):
        name    = item["name"]
        item_id = item["id"]
        cv_map  = {cv["id"]: cv for cv in item["column_values"]}

        website_cv = cv_map.get(website_col["id"])
        followers_cv = cv_map.get(followers_col["id"])

        website_url = extract_website_from_link_cv(website_cv)
        current_val = (followers_cv or {}).get("text") or ""

        print(f"\n[{idx}] {name} (id={item_id})")
        print(f"    Website       = {website_url}")
        print(f"    Current value = {current_val!r}")

        # Skip if already has a value
        if current_val.strip():
            print("    → Followers Count already set; skipping.")
            continue

        if not website_url:
            print("    → No website; skipping.")
            continue

        base_url = normalize_base_url(website_url)
        if not base_url:
            print("    → Could not normalize website; skipping.")
            continue

        print(f"    Base URL      = {base_url}")

        html = fetch_html(base_url)
        if not html:
            print("    → Failed to fetch homepage; skipping.")
            continue

        social_links = find_social_links(base_url, html)
        if not social_links:
            print("    → No social links found; skipping.")
            continue

        print(f"    Found social links: {social_links}")

        total_followers = 0
        any_found = False

        for platform, url in social_links.items():
            print(f"      [CHECK] {platform}: {url}")
            count = get_followers_for_platform(platform, url)
            if count is not None and count > 0:
                print(f"        → {count} followers")
                total_followers += count
                any_found = True
            else:
                print("        → followers not found / not visible")

        if not any_found:
            print("    → No follower counts could be determined; skipping update.")
            continue

        print(f"    Total followers (sum across platforms): {total_followers}")

        try:
            update_number_column(MONDAY_BOARD_ID, item_id, followers_col["id"], total_followers)
            print("    ✅ Followers Count updated.")
        except Exception as e:
            print(f"    ❌ Update failed: {e}")

        time.sleep(1.0)

    print("\nDone ✅")


if __name__ == "__main__":
    main()