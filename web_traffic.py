#!/usr/bin/env python3
"""
website_traffic_score.py

Instead of scraping blocked 3rd-party tools, this script computes a *rough*
internal "traffic score" based on each company's own site:

  score ≈ (number of internal pages crawled) * (average words per page)

It then writes that numeric score into the Numbers column:
  "Website Traffic Estimate"

This is NOT real visits/month, but gives you a consistent relative scale across
competitors using only their websites.

Requirements:
    pip install python-dotenv requests beautifulsoup4
"""

import os
import json
import time
from typing import Dict, Any, List, Optional, Set
from urllib.parse import urlparse, urljoin

import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# ========= CONFIG =========
load_dotenv()

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE  = "Website"                   # Link column
TRAFFIC_COL_TITLE  = "Website Traffic Estimate"  # Numbers column

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

MAX_PAGES_PER_SITE = 10   # cap internal pages to crawl per site


# ========= Monday helpers =========

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    resp = requests.post(
        MONDAY_URL, headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]


def fetch_board(board_id: str) -> Dict[str, Any]:
    q = """
    query($ids:[ID!]){
      boards(ids:$ids){
        name
        columns { id title type settings_str }
      }
    }
    """
    return gql(q, {"ids":[board_id]})["boards"][0]


def fetch_items(board_id: str) -> List[Dict[str, Any]]:
    q = """
    query($board_id:[ID!], $cursor:String){
      boards(ids:$board_id){
        items_page(limit:500, cursor:$cursor){
          items{
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
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None


def update_number_column(board_id: str, item_id: str, col_id: str, number_value: float) -> None:
    vals = {col_id: str(number_value)}
    m = """
    mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(
        board_id:$board_id,
        item_id:$item_id,
        column_values:$vals
      ){
        id
      }
    }
    """
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})


def extract_website_from_link_cv(cv: Dict[str, Any]) -> str:
    if not cv:
        return ""
    raw_val = cv.get("value")
    if raw_val:
        try:
            j = json.loads(raw_val)
            url = (j.get("url") or "").strip()
            if url:
                return url
        except Exception:
            pass
    return (cv.get("text") or "").strip()


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
        # strip query/fragment
        base = parsed._replace(path="", query="", fragment="").geturl()
        return base
    except Exception:
        return None


# ========= Site crawling / “traffic score” =========

def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [WARN] fetch_html failed for {url}: {e}")
        return None


def extract_internal_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    parsed_base = urlparse(base_url)
    base_netloc = parsed_base.netloc.lower()

    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        abs_url = urljoin(base_url, href)
        try:
            parsed = urlparse(abs_url)
        except Exception:
            continue
        if parsed.netloc.lower() != base_netloc:
            continue
        # strip query/fragment
        clean = parsed._replace(query="", fragment="").geturl()
        if clean not in links:
            links.append(clean)
    return links


def word_count(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    if not text:
        return 0
    return len(text.split())


def compute_traffic_score(base_url: str) -> float:
    """
    Crawl up to MAX_PAGES_PER_SITE internal pages starting from base_url,
    compute a rough score = internal_pages * avg_words_per_page.
    """
    visited: Set[str] = set()
    to_visit: List[str] = [base_url]
    total_words = 0
    pages_crawled = 0

    while to_visit and pages_crawled < MAX_PAGES_PER_SITE:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        print(f"      [CRAWL] {url}")
        html = fetch_html(url)
        if not html:
            continue

        pages_crawled += 1
        wc = word_count(html)
        total_words += wc

        # Add more internal links
        internal = extract_internal_links(base_url, html)
        for link in internal:
            if link not in visited and link not in to_visit:
                to_visit.append(link)

    if pages_crawled == 0:
        return 0.0

    avg_words = total_words / pages_crawled
    score = pages_crawled * avg_words
    return score


# ========= MAIN =========

def main():
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        raise SystemExit("❌ Missing MONDAY_API_TOKEN or MONDAY_BOARD_ID in .env")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]
    print(f"[INFO] Board: {board['name']}")

    website_col = col_by_title(cols, WEBSITE_COL_TITLE)
    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")

    traffic_col = col_by_title(cols, TRAFFIC_COL_TITLE)
    if not traffic_col:
        raise SystemExit(f"❌ Column '{TRAFFIC_COL_TITLE}' not found.")

    print(f"[INFO] Website column id            = {website_col['id']} (type={website_col['type']})")
    print(f"[INFO] Website Traffic Estimate col = {traffic_col['id']} (type={traffic_col['type']})")
    print(f"[INFO] MAX_PAGES_PER_SITE           = {MAX_PAGES_PER_SITE}")
    print()

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items fetched: {len(items)}\n")

    for i, item in enumerate(items, 1):
        name    = item["name"]
        item_id = item["id"]
        cv_map  = {cv["id"]: cv for cv in item["column_values"]}

        website_cv   = cv_map.get(website_col["id"])
        website_url  = extract_website_from_link_cv(website_cv)
        current_text = cv_map.get(traffic_col["id"], {}).get("text") or ""

        print(f"[{i}] {name} (id={item_id})")
        print(f"    Website: {website_url!r}")
        print(f"    Current traffic score (text): {current_text!r}")

        # Skip if already set
        if current_text.strip():
            print("    → Already has value; skipping.\n")
            continue

        if not website_url.strip():
            print("    → No website; skipping.\n")
            continue

        base_url = normalize_base_url(website_url)
        if not base_url:
            print("    → Could not normalize base URL; skipping.\n")
            continue

        print(f"    Base URL: {base_url}")

        score = compute_traffic_score(base_url)
        score_int = int(round(score))
        print(f"    Computed traffic score: {score_int}")

        try:
            update_number_column(MONDAY_BOARD_ID, item_id, traffic_col["id"], score_int)
            print("    ✅ Traffic score updated.\n")
        except Exception as e:
            print(f"    ❌ Update failed: {e}\n")

        time.sleep(1.0)

    print("Done ✅")


if __name__ == "__main__":
    main()