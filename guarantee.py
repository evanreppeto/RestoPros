#!/usr/bin/env python3
"""
service_guarantees.py

Scrapes each company's website and extracts service guarantees,
then updates the Monday.com text column "Service Guarantees".

- If the column already has a value, it skips that company.
- If guarantees are found, inserts extracted text.
- If none found, writes: "None found".

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
from dotenv import load_dotenv


# ================= CONFIG ==================
load_dotenv()


MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE   = "Website"
TARGET_COL_TITLE    = "Service Guarantees"   # TEXT COLUMN

MONDAY_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json"
}

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
SCRAPE_TIMEOUT = 10


# ================= Monday Helpers ==================

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(
        MONDAY_URL, headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    r.raise_for_status()
    d = r.json()
    if "errors" in d:
        raise RuntimeError(json.dumps(d["errors"], indent=2))
    return d["data"]


def fetch_board(board_id: str):
    q = """
    query($ids:[ID!]){
      boards(ids:$ids){
        name
        columns { id title type }
      }
    }
    """
    return gql(q, {"ids":[board_id]})["boards"][0]


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


def col_by_title(columns, title):
    title = title.strip().lower()
    for c in columns:
        if c["title"].strip().lower() == title:
            return c
    return None


def update_text_column(board_id, item_id, col_id, value):
    mutation = """
    mutation($b:ID!, $i:ID!, $vals:JSON!){
      change_multiple_column_values(
        board_id:$b,
        item_id:$i,
        column_values:$vals
      ){ id }
    }
    """
    gql(mutation, {"b":board_id, "i":item_id, "vals":json.dumps({col_id:value})})


# ================= Scraping Logic ==================

def extract_website(link_cv) -> Optional[str]:
    if not link_cv:
        return None
    raw_val = link_cv.get("value")
    if raw_val:
        try:
            j = json.loads(raw_val)
            url = j.get("url")
            if url:
                return url
        except:
            pass
    return None


KEYWORDS = [
    "guarantee", "guaranteed",
    "warranty",
    "satisfaction", "100%", "promise",
    "quality assurance",
    "lifetime", "peace of mind",
    "service guarantee",
    "workmanship guarantee",
]


def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=SCRAPE_HEADERS, timeout=SCRAPE_TIMEOUT)
        r.raise_for_status()
        return r.text
    except:
        return None


def find_guarantees(html: str) -> Optional[str]:
    """
    Look for short, guarantee-related snippets instead of dumping the whole page.

    Strategy:
      - Scan <p>, <li>, and headings for keywords (guarantee, warranty, satisfaction, etc.)
      - Keep only reasonably short lines
      - Return up to 3 unique snippets joined with " | "
    """
    soup = BeautifulSoup(html, "lxml")

    # Elements where guarantee language usually lives
    candidates = soup.find_all(["p", "li", "h1", "h2", "h3", "h4", "span"])

    snippets: List[str] = []

    for el in candidates:
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue

        lower = txt.lower()
        if not any(k in lower for k in KEYWORDS):
            continue

        # Clean up whitespace
        txt = " ".join(txt.split())

        # Skip nav/menu garbage that just repeats the whole site
        if len(txt) < 15:
            continue
        if len(txt) > 240:
            txt = txt[:237] + "..."

        if txt not in snippets:
            snippets.append(txt)

        if len(snippets) >= 3:   # cap how much we stuff into the column
            break

    if not snippets:
        return None

    # Join into one cell value like: "100% Satisfaction Guaranteed | Always Free Estimates"
    return " | ".join(snippets)


# ================= MAIN ==================

def main():
    print("Running Service Guarantees scraper…")

    board = fetch_board(MONDAY_BOARD_ID)
    cols = board["columns"]

    website_col = col_by_title(cols, WEBSITE_COL_TITLE)
    target_col  = col_by_title(cols, TARGET_COL_TITLE)

    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")
    if not target_col:
        raise SystemExit(f"❌ Text column '{TARGET_COL_TITLE}' not found.")

    print(f"[INFO] Using Website col: {website_col['id']}")
    print(f"[INFO] Using Service Guarantees col: {target_col['id']}")

    items = fetch_items(MONDAY_BOARD_ID)

    for idx, item in enumerate(items, 1):
        name = item["name"]
        item_id = item["id"]

        cv_map = {cv["id"]: cv for cv in item["column_values"]}

        website = extract_website(cv_map.get(website_col["id"]))
        current = (cv_map.get(target_col["id"], {}) or {}).get("text") or ""

        print(f"\n[{idx}] {name}")
        print(f"    Website = {website}")
        print(f"    Current = {current!r}")

        # Skip if already filled
        if current.strip():
            print("    → Already filled, skipping.")
            continue

        if not website:
            print("    → No website found, setting: None found")
            update_text_column(MONDAY_BOARD_ID, item_id, target_col["id"], "None found")
            continue

        html = fetch_html(website)
        if not html:
            print("    → Could not load site, writing None found.")
            update_text_column(MONDAY_BOARD_ID, item_id, target_col["id"], "None found")
            continue

        found = find_guarantees(html)
        if not found:
            print("    → No guarantees detected.")
            update_text_column(MONDAY_BOARD_ID, item_id, target_col["id"], "None found")
        else:
            print(f"    → Service Guarantees found: {found}")
            update_text_column(MONDAY_BOARD_ID, item_id, target_col["id"], found)

        time.sleep(0.8)

    print("\nDone ✅")


if __name__ == "__main__":
    main()