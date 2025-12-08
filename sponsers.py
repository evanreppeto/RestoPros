#!/usr/bin/env python3
"""
sponsorships_snippets.py

For each company on a Monday.com board:
  - Read the Website column (Link)
  - Crawl homepage + a few internal pages
  - Look for lines/sentences that mention sponsorship/community partners
  - Write those snippets into the text column "Sponsorships"
      e.g. "Proud sponsor of XYZ | Community partner of ABC"
  - If nothing found -> "None"

This is intentionally SIMPLE and literal: we save the matching text instead of
trying to guess sponsor names with regex magic.

Requires:
    pip install python-dotenv requests beautifulsoup4
"""

import os
import json
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urljoin

import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# ========= CONFIG =========
load_dotenv()

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE      = "Website"      # Link column title
SPONSORSHIP_COL_TITLE  = "Sponsorships" # Text column title

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
REQUEST_TIMEOUT = 12
MAX_EXTRA_PAGES = 5   # internal pages per site to check

# Keywords that indicate sponsorship / community partnership
SPONSORSHIP_KEYWORDS = [
    "sponsor", "sponsors", "sponsorship", "sponsoring", "sponsored by",
    "proud sponsor", "proud to sponsor", "community partner",
    "community partners", "our partners", "partnered with",
    "charity partner", "charitable partner", "nonprofit partner",
    "foundation partner", "local team", "youth sports", "little league",
    "supporting our community", "support our community",
    "community involvement", "community outreach", "community engagement",
]

# ---------- Monday API helpers ----------

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(
        MONDAY_URL,
        headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
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
            column_values { id text value }
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


def update_text_column(board_id: str, item_id: str, col_id: str, text_value: str) -> None:
    vals = {col_id: text_value}
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


def extract_website(column_value: Dict[str, Any], col_type: str) -> str:
    """
    For a Monday Link column:
      - Prefer JSON value.url
      - Fallback to text
    For other text-based columns:
      - Use text
    """
    if not column_value:
        return ""
    if (col_type or "").lower() == "link":
        raw = column_value.get("value")
        if raw:
            try:
                j = json.loads(raw)
                url = (j.get("url") or "").strip()
                if url:
                    return url
            except Exception:
                pass
        return (column_value.get("text") or "").strip()
    return (column_value.get("text") or "").strip()


# ---------- Web scraping helpers ----------

def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    try:
        parsed = urlparse(u)
        return u if parsed.netloc else None
    except Exception:
        return None


def fetch_page_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [WARN] Failed to fetch {url}: {e}")
        return None


def find_internal_sponsorship_pages(base_url: str, home_html: str) -> List[str]:
    soup = BeautifulSoup(home_html, "lxml")
    parsed = urlparse(base_url)
    base_netloc = parsed.netloc.lower()

    candidates: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        abs_url = urljoin(base_url, href)
        try:
            if urlparse(abs_url).netloc.lower() != base_netloc:
                continue
        except Exception:
            continue

        href_lower = abs_url.lower()
        if any(word in href_lower for word in ["sponsor", "community", "partner", "about", "giving", "charity"]):
            candidates.append(abs_url)

    # Deduplicate & cap
    seen = set()
    uniq: List[str] = []
    for u in candidates:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
        if len(uniq) >= MAX_EXTRA_PAGES:
            break

    return uniq


def extract_sponsorship_snippets_from_html(html: str) -> List[str]:
    """
    Return human-readable text snippets that mention sponsorship/partners.
    We look at paragraphs, list items, and headings.
    """
    soup = BeautifulSoup(html, "lxml")
    snippets: List[str] = []

    elements = soup.find_all(["p", "li", "h1", "h2", "h3", "h4"])
    for el in elements:
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        lower = txt.lower()
        if any(kw in lower for kw in SPONSORSHIP_KEYWORDS):
            # Keep things reasonable length
            if len(txt) > 300:
                txt = txt[:297] + "..."
            if txt not in snippets:
                snippets.append(txt)

    return snippets


def detect_sponsorship_snippets(url: str) -> List[str]:
    """
    Crawl homepage + a few internal pages and collect snippets that
    mention sponsorship/community partners.
    """
    norm = normalize_url(url)
    if not norm:
        return []

    all_snips: List[str] = []

    # Homepage
    print(f"      [SCAN] homepage: {norm}")
    home_html = fetch_page_html(norm)
    if not home_html:
        return []

    home_snips = extract_sponsorship_snippets_from_html(home_html)
    all_snips.extend(home_snips)

    # Internal pages likely to contain sponsorship info
    extra_pages = find_internal_sponsorship_pages(norm, home_html)
    for idx, page_url in enumerate(extra_pages, 1):
        print(f"      [SCAN] extra page {idx}: {page_url}")
        page_html = fetch_page_html(page_url)
        if not page_html:
            continue
        page_snips = extract_sponsorship_snippets_from_html(page_html)
        for s in page_snips:
            if s not in all_snips:
                all_snips.append(s)

    return all_snips


# ---------- MAIN ----------

def main():
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        raise SystemExit("❌ Missing MONDAY_API_TOKEN or MONDAY_BOARD_ID in .env")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]

    website_col = col_by_title(cols, WEBSITE_COL_TITLE)
    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")

    sponsorship_col = col_by_title(cols, SPONSORSHIP_COL_TITLE)
    if not sponsorship_col:
        raise SystemExit(f"❌ Text column '{SPONSORSHIP_COL_TITLE}' not found.")

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Website      column id = {website_col['id']}")
    print(f"[INFO] Sponsorships column id = {sponsorship_col['id']}")
    print()

    items = fetch_items(MONDAY_BOARD_ID)

    for i, item in enumerate(items, 1):
        name = item["name"]
        cv_map = {c["id"]: c for c in item["column_values"]}

        current_val = cv_map.get(sponsorship_col["id"], {}).get("text") or ""
        website_val = extract_website(cv_map.get(website_col["id"]), website_col["type"])

        print(f"[{i}] {name}")
        print(f"     Website: {website_val!r}")
        print(f"     Current Sponsorships: {current_val!r}")

        if not website_val.strip():
            snippets = []
        else:
            snippets = detect_sponsorship_snippets(website_val)

        if snippets:
            new_value = " | ".join(snippets)
        else:
            new_value = "None"

        print(f"     Detected Sponsorships: {new_value!r}")

        # Only update if changed
        if current_val.strip() != new_value:
            try:
                update_text_column(MONDAY_BOARD_ID, item["id"], sponsorship_col["id"], new_value)
                print("     ✅ Updated")
            except Exception as e:
                print(f"     ❌ Update failed: {e}")
        else:
            print("     (unchanged)")

        print()
        time.sleep(1.0)

    print("Done ✅")


if __name__ == "__main__":
    main()