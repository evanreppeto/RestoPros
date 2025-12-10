#!/usr/bin/env python3
"""
ad_samples_files_from_ads_transparency.py

For each item on a Monday.com board:
  - Read the Website column (Link)
  - Extract the website's domain
  - Build the Google Ads Transparency URL:
        https://adstransparency.google.com/?region=anywhere&domain=<domain>
  - Insert that URL as a LINK-type "file" into the "Ad Samples" Files column.

This does NOT upload actual files; it adds link entries to the Files column.

Requirements:
    pip install python-dotenv requests

.env must include:
    MONDAY_API_TOKEN=...
    MONDAY_BOARD_ID=...
"""

import os
import json
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

# ============== CONFIG ==============

load_dotenv()
TARGET_ITEM_ID   = os.getenv("TARGET_ITEM_ID")
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE  = "Website"     # Link column title
AD_SAMPLES_TITLE   = "Ad Samples"  # Files column title

# If True, we WILL overwrite whatever is in the Ad Samples column.
# If False, we'll skip rows where the files column is already non-empty.
OVERWRITE_EXISTING = False

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN or "",
    "Content-Type": "application/json",
}


# ============== Monday helpers ==============

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    """Small helper to call Monday GraphQL."""
    resp = requests.post(
        MONDAY_API_URL,
        headers=MONDAY_HEADERS,
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
    return gql(q, {"ids": [board_id]})["boards"][0]


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
        d = gql(q, {"board_id": [board_id], "cursor": cursor})
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


def extract_website_from_link_cv(cv: Dict[str, Any]) -> str:
    """
    Monday Link column:
      cv["value"] is JSON like {"url": "...", "text": "Website"}
      cv["text"] might be "Website - https://...".
    Prefer value.url.
    """
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


def normalize_domain(url: str) -> Optional[str]:
    """
    From a full URL, extract bare domain: e.g.,
      https://redefinedresto.com/water-damage -> redefinedresto.com
    """
    if not url:
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    try:
        parsed = urlparse(u)
        if not parsed.netloc:
            return None
        host = parsed.netloc.lower()
        # strip www.
        if host.startswith("www."):
            host = host[4:]
        # remove port if any
        host = host.split(":")[0]
        return host or None
    except Exception:
        return None


def build_ads_transparency_url(domain: str) -> str:
    return f"https://adstransparency.google.com/?region=anywhere&domain={domain}"


def files_column_has_any_files(cv: Dict[str, Any]) -> bool:
    """
    For FileValue, text is typically non-empty if there are files.
    But we prefer to inspect raw 'value' JSON if present.
    """
    if not cv:
        return False
    raw = cv.get("value")
    if raw:
        try:
            j = json.loads(raw)
            files = j.get("files") or []
            return len(files) > 0
        except Exception:
            pass
    # fallback to text
    text = cv.get("text") or ""
    return bool(text.strip())


def set_files_column_link(board_id: str, item_id: str, column_id: str, link_url: str, link_name: str) -> None:
    """
    Use change_column_value with a 'files' array where fileType='LINK'.
    This attaches a link into the Files column (no upload).
    """
    value_obj = {
        "files": [
            {
                "name": link_name,
                "fileType": "LINK",
                "linkToFile": link_url,
            }
        ]
    }
    value_str = json.dumps(value_obj)

    m = """
    mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {
      change_column_value(
        board_id: $board_id,
        item_id: $item_id,
        column_id: $column_id,
        value: $value
      ) {
        id
      }
    }
    """
    gql(
        m,
        {
            "board_id": board_id,
            "item_id": item_id,
            "column_id": column_id,
            "value": value_str,
        },
    )


# ============== MAIN ==============

def main():
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        raise SystemExit("❌ Set MONDAY_API_TOKEN and MONDAY_BOARD_ID in your .env")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]
    print(f"[INFO] Board: {board['name']}")

    website_col = col_by_title(cols, WEBSITE_COL_TITLE)
    if not website_col:
        raise SystemExit(f"❌ Website column '{WEBSITE_COL_TITLE}' not found.")

    ad_files_col = col_by_title(cols, AD_SAMPLES_TITLE)
    if not ad_files_col:
        raise SystemExit(f"❌ Files column '{AD_SAMPLES_TITLE}' not found.")
    if ad_files_col["type"].lower() not in ("file", "files"):
        raise SystemExit(f"❌ Column '{AD_SAMPLES_TITLE}' is not a Files column (type={ad_files_col['type']}).")

    print(f"[INFO] Website column id   = {website_col['id']} (type={website_col['type']})")
    print(f"[INFO] Ad Samples column id = {ad_files_col['id']} (type={ad_files_col['type']})")
    print(f"[INFO] Overwrite existing files? {OVERWRITE_EXISTING}")
    print()

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items fetched: {len(items)}\n")

    if TARGET_ITEM_ID:
        filtered = [it for it in items if str(it["id"]) == str(TARGET_ITEM_ID)]
        print(f"[INFO] Filtered to TARGET_ITEM_ID={TARGET_ITEM_ID} → {len(filtered)} matching item(s)")
        items = filtered
        if not items:
            print("[INFO] No matching item for TARGET_ITEM_ID; nothing to do.")
            return

    for i, item in enumerate(items, 1):
        name = item["name"]
        item_id = item["id"]

        cv_map = {cv["id"]: cv for cv in item["column_values"]}

        website_cv = cv_map.get(website_col["id"])
        website_url = extract_website_from_link_cv(website_cv)

        ad_files_cv = cv_map.get(ad_files_col["id"])

        print(f"[{i}] {name} (id={item_id})")
        print(f"    Website raw: {website_url!r}")

        if not website_url.strip():
            print("    → No website URL; skipping.\n")
            continue

        if not OVERWRITE_EXISTING and files_column_has_any_files(ad_files_cv):
            print("    → Ad Samples already has files; skipping (OVERWRITE_EXISTING=False).\n")
            continue

        domain = normalize_domain(website_url)
        if not domain:
            print("    → Could not parse domain; skipping.\n")
            continue

        ads_url = build_ads_transparency_url(domain)
        link_name = f"Google Ads - {domain}"

        print(f"    Parsed domain: {domain}")
        print(f"    Ads URL      : {ads_url}")

        try:
            set_files_column_link(MONDAY_BOARD_ID, item_id, ad_files_col["id"], ads_url, link_name)
            print("    ✅ Files column updated with link.\n")
        except Exception as e:
            print(f"    ❌ Update failed: {e}\n")

        # Small delay to be gentle with API
        time.sleep(0.5)

    print("Done ✅")


if __name__ == "__main__":
    main()