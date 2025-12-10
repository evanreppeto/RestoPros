#!/usr/bin/env python3
"""
organic_keywords.py

For each company on a Monday.com board:

  - Read the Website column (Link)
  - Fetch the homepage HTML
  - Extract visible text
  - Compute the most frequent "keywords" on the page (excluding stopwords, very short words, etc.)
  - Write the top N keywords as a comma-separated string into the text column:
        "Organic Keywords" (or "Top Organic Keywords")

Behavior:
  - Skips items where the Organic Keywords column is already non-empty.
  - If the site cannot be fetched or yields no usable text, writes "None found".

Requirements:
    pip install python-dotenv requests beautifulsoup4
"""

import os
import json
import time
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from collections import Counter
from dotenv import load_dotenv

# ============== LOAD .env FROM SCRIPT DIR ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")

# ================= CONFIG ==================
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE      = "Website"   # Link column title
ORGANIC_COL_CANDIDATES = ["Organic Keywords", "Top Organic Keywords"]  # will pick the first that exists

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

TOP_N_KEYWORDS = 10  # how many keywords to store

# Very simple English stopword list (good enough for this use case)
STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "your", "you",
    "are", "our", "have", "has", "was", "were", "will", "can", "not",
    "but", "all", "any", "about", "into", "more", "most", "other",
    "over", "such", "than", "then", "also", "they", "them", "their",
    "there", "here", "what", "when", "where", "why", "how", "who",
    "which", "within", "between", "been", "being", "out", "up", "down",
    "on", "in", "of", "to", "as", "by", "at", "it", "its", "a", "an",
    "or", "is", "we", "i", "me", "my", "ours"
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


def pick_organic_col(columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for candidate in ORGANIC_COL_CANDIDATES:
        col = col_by_title(columns, candidate)
        if col:
            return col
    return None


def update_text_column(board_id: str, item_id: str, col_id: str, text_value: str) -> None:
    mutation = """
    mutation($b:ID!, $i:ID!, $vals:JSON!){
      change_multiple_column_values(
        board_id:$b,
        item_id:$i,
        column_values:$vals
      ){ id }
    }
    """
    gql(mutation, {"b":board_id, "i":item_id, "vals":json.dumps({col_id: text_value})})


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
    txt = (cv.get("text") or "").strip()
    return txt or None


# ================= HTTP / Text Helpers ==================

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
        clean = parsed._replace(query="", fragment="").geturl()
        return clean
    except Exception:
        return None


def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [WARN] fetch_html failed for {url}: {e}")
        return None


def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Remove things that are unlikely to be content
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    # squeeze whitespace
    return " ".join(text.split())


def extract_keywords(text: str, top_n: int = TOP_N_KEYWORDS) -> List[str]:
    """
    Very simple keyword extraction:
      - Lowercase
      - Split on non-alphabetic characters
      - Remove stopwords, very short tokens, and numbers
      - Return top N by frequency
    """
    # Keep only letters, replace everything else with space
    text = text.lower()
    text = re.sub(r"[^a-z]+", " ", text)
    tokens = text.split()

    filtered: List[str] = []
    for tok in tokens:
        if len(tok) < 4:              # skip very short words
            continue
        if tok in STOPWORDS:
            continue
        filtered.append(tok)

    if not filtered:
        return []

    counts = Counter(filtered)
    # Most common, but keep unique ordering by frequency
    top = [w for (w, _) in counts.most_common(top_n)]
    return top


# ================= MAIN ==================

def main():
    if not MONDAY_BOARD_ID:
        raise SystemExit("❌ MONDAY_BOARD_ID is empty. Check your .env file.")

    print("Running Organic Keywords scraper…")
    print(f"[DEBUG] Using .env at: {ENV_PATH}")
    print(f"[DEBUG] MONDAY_BOARD_ID={MONDAY_BOARD_ID}")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]

    website_col = col_by_title(cols, WEBSITE_COL_TITLE)
    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")

    organic_col = pick_organic_col(cols)
    if not organic_col:
        raise SystemExit(f"❌ Could not find an 'Organic Keywords' column. "
                         f"Tried: {ORGANIC_COL_CANDIDATES}")

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Website col id         = {website_col['id']} (type={website_col['type']})")
    print(f"[INFO] Organic Keywords col   = {organic_col['title']} (id={organic_col['id']}, type={organic_col['type']})")

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

        website_cv   = cv_map.get(website_col["id"])
        organic_cv   = cv_map.get(organic_col["id"])

        website_url = extract_website_from_link_cv(website_cv)
        current_val = (organic_cv or {}).get("text") or ""

        print(f"\n[{idx}] {name} (id={item_id})")
        print(f"    Website         = {website_url}")
        print(f"    Current keywords= {current_val!r}")

        # Skip if already set
        if current_val.strip():
            print("    → Organic Keywords already set; skipping.")
            continue

        if not website_url:
            print("    → No website URL; writing 'None found'.")
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], "None found")
            continue

        base_url = normalize_base_url(website_url)
        if not base_url:
            print("    → Could not normalize website; writing 'None found'.")
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], "None found")
            continue

        print(f"    Base URL        = {base_url}")

        html = fetch_html(base_url)
        if not html:
            print("    → Failed to fetch homepage; writing 'None found'.")
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], "None found")
            continue

        text = extract_visible_text(html)
        if not text:
            print("    → No visible text extracted; writing 'None found'.")
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], "None found")
            continue

        keywords = extract_keywords(text, TOP_N_KEYWORDS)
        if not keywords:
            print("    → No keywords extracted; writing 'None found'.")
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], "None found")
            continue

        keywords_str = ", ".join(keywords)
        print(f"    Extracted keywords: {keywords_str}")

        try:
            update_text_column(MONDAY_BOARD_ID, item_id, organic_col["id"], keywords_str)
            print("    ✅ Organic Keywords updated.")
        except Exception as e:
            print(f"    ❌ Update failed: {e}")

        time.sleep(0.8)

    print("\nDone ✅")


if __name__ == "__main__":
    main()