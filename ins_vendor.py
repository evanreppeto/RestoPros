#!/usr/bin/env python3
"""
insurance_vendor_status_simple.py

For each company on a Monday.com board:
  - Read the Website column (Link)
  - Fetch the homepage + a few insurance-related internal pages
  - Look for:
      * insurance-vendor phrases
      * common insurance carrier names
  - If any found  -> Status 'Insurance Vendor' = Yes
    Else          -> Status 'Insurance Vendor' = No

Uses:
  MONDAY_API_TOKEN, MONDAY_BOARD_ID from .env

Requires:
  pip install python-dotenv requests beautifulsoup4 tldextract
"""

import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# ========= CONFIG =========
load_dotenv()
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_TITLE          = "Website"           # Website Link column title
INSURANCE_STATUS_TITLE = "Insurance Vendor" # Status column (Yes / No)

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN or "",
    "Content-Type": "application/json",
}

SITE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT = 12
MAX_EXTRA_PAGES = 3  # extra internal pages per site to check

# Phrases that strongly indicate "we work with insurance"
INSURANCE_PHRASES = [
    "work with your insurance",
    "we work with your insurance",
    "work with all insurance companies",
    "work with all major insurance",
    "deal directly with your insurance",
    "bill your insurance",
    "we bill your insurance",
    "directly with the insurance company",
    "insurance claims assistance",
    "insurance claim assistance",
    "insurance claim process",
    "insurance company approved",
    "approved by your insurance",
    "assist with your insurance",
    "we handle the insurance",
]

# Known carrier keywords (looser signal)
INSURANCE_CARRIERS: Dict[str, str] = {
    "state farm": "State Farm",
    "allstate": "Allstate",
    "farmers insurance": "Farmers Insurance",
    "nationwide": "Nationwide",
    "progressive": "Progressive",
    "liberty mutual": "Liberty Mutual",
    "usaa": "USAA",
    "traveler": "Travelers",
    "travellers": "Travelers",
    "chubb": "Chubb",
    "the hartford": "The Hartford",
    "hartford insurance": "The Hartford",
    "geico": "GEICO",
    "american family": "American Family Insurance",
    "amfam": "American Family Insurance",
    "metlife": "MetLife",
    "erie insurance": "Erie Insurance",
    "auto-owners insurance": "Auto-Owners Insurance",
}
# ==========================

# ---------- Monday helpers ----------
def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    resp = requests.post(
        MONDAY_API_URL,
        headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=30,
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
        data = gql(q, {"board_id": [board_id], "cursor": cursor})
        page = data["boards"][0]["items_page"]
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


def status_label_indices(settings_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    From Status settings_str, find indices for Yes / No labels.
    """
    if not settings_str:
        return (None, None)
    try:
        s = json.loads(settings_str)
        labels = s.get("labels") or {}
        name_to_idx = {
            (v or "").strip().lower(): int(k)
            for k, v in labels.items() if isinstance(v, str)
        }
        yes_idx = next((name_to_idx[k] for k in ("yes", "true", "active") if k in name_to_idx), None)
        no_idx  = next((name_to_idx[k] for k in ("no", "false", "inactive") if k in name_to_idx), None)
        return (yes_idx, no_idx)
    except Exception:
        return (None, None)


def set_status_by_index(board_id: str, item_id: str, column_id: str, index: int) -> None:
    vals = {column_id: {"index": index}}
    m = """
    mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){
        id
      }
    }
    """
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})


def set_status_by_label(board_id: str, item_id: str, column_id: str, label_text: str) -> None:
    vals = {column_id: {"label": label_text}}
    m = """
    mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){
        id
      }
    }
    """
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})


def extract_website_url(column_value: Dict[str, Any], col_type: str) -> str:
    """
    For a Monday Link column:
      - Prefer value.url
      - Fallback to text
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


# ---------- Site helpers ----------
def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    try:
        p = urlparse(u)
        if not p.netloc:
            return None
        return u
    except Exception:
        return None


def same_site(url: str, base_netloc: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == base_netloc.lower()
    except Exception:
        return False


def fetch_page(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=SITE_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text(" ", strip=True)
        return text[:30000]
    except Exception as e:
        print(f"      [WARN] Failed to fetch {url}: {e}")
        return None


def page_has_insurance_signal(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()

    for phrase in INSURANCE_PHRASES:
        if phrase in lower:
            print(f"      [MATCH] phrase: {phrase!r}")
            return True

    for key in INSURANCE_CARRIERS.keys():
        if key in lower:
            print(f"      [MATCH] carrier: {key!r}")
            return True

    return False


def site_has_insurance_vendor(start_url: str) -> bool:
    """
    Check homepage + a few internal pages that look like insurance/claims pages.
    """
    start = normalize_url(start_url)
    if not start:
        return False

    parsed = urlparse(start)
    base_netloc = parsed.netloc.lower()

    print(f"      [SCAN] homepage: {start}")
    home_text = fetch_page(start)
    if page_has_insurance_signal(home_text or ""):
        return True

    if not home_text:
        return False

    # Look for candidate internal links containing insurance-ish words in the URL
    soup = BeautifulSoup(home_text, "lxml")
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        abs_url = urljoin(start, href)
        if not same_site(abs_url, base_netloc):
            continue
        href_lower = abs_url.lower()
        if any(key in href_lower for key in ("insur", "claim", "claims", "faq", "services")):
            candidates.append(abs_url)

    # Deduplicate and cap
    seen = set()
    unique_candidates = []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            unique_candidates.append(u)
        if len(unique_candidates) >= MAX_EXTRA_PAGES:
            break

    for idx, url in enumerate(unique_candidates, 1):
        print(f"      [SCAN] extra page {idx}: {url}")
        txt = fetch_page(url)
        if page_has_insurance_signal(txt or ""):
            return True

    return False


# ---------- Main ----------
def main():
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        raise SystemExit("❌ Set MONDAY_API_TOKEN and MONDAY_BOARD_ID in your .env file.")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]
    print(f"[INFO] Board: {board['name']}")

    website_col = col_by_title(cols, WEBSITE_TITLE)
    if not website_col:
        raise SystemExit(f"❌ Column '{WEBSITE_TITLE}' not found.")

    status_col = col_by_title(cols, INSURANCE_STATUS_TITLE)
    if not status_col:
        raise SystemExit(f"❌ Status column '{INSURANCE_STATUS_TITLE}' not found.")
    status_type = (status_col.get("type") or "").lower()
    if status_type not in ("color", "status"):
        raise SystemExit(f"❌ Column '{INSURANCE_STATUS_TITLE}' must be a Status type (currently '{status_type}').")

    yes_idx, no_idx = status_label_indices(status_col.get("settings_str"))
    if yes_idx is None or no_idx is None:
        print("[WARN] Could not automatically find 'Yes'/'No' indices from labels.")
        print("       Make sure the 'Insurance Vendor' status has labels named 'Yes' and 'No'.")

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items: {len(items)}\n")

    if TARGET_ITEM_ID:
        filtered = [it for it in items if str(it["id"]) == str(TARGET_ITEM_ID)]
        print(f"[INFO] Filtered to TARGET_ITEM_ID={TARGET_ITEM_ID} → {len(filtered)} matching item(s)")
        items = filtered
        if not items:
            print("[INFO] No matching item for TARGET_ITEM_ID; nothing to do.")
            return

    for i, it in enumerate(items, 1):
        name = it["name"]
        cv_map = {c["id"]: c for c in it["column_values"]}

        current_text = (cv_map.get(status_col["id"], {}) or {}).get("text") or ""
        current_lc   = current_text.strip().lower()

        website_cv   = cv_map.get(website_col["id"])
        website_url  = extract_website_url(website_cv, website_col["type"])

        print(f"[{i}] {name}")
        print(f"      current status: {current_text!r}")
        print(f"      website value : {website_url!r}")

        if not website_url.strip():
            print("      no website → setting No")
            desired = "no"
        else:
            has_vendor = site_has_insurance_vendor(website_url)
            desired = "yes" if has_vendor else "no"
            print(f"      detected: {desired.upper()}")

        # Only update if different
        if current_lc == desired:
            print("      already correct, skipping update.")
            print()
            continue

        try:
            if desired == "yes":
                if yes_idx is not None:
                    set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], yes_idx)
                else:
                    set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "Yes")
                print("      ✅ set Insurance Vendor → Yes")
            else:
                if no_idx is not None:
                    set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], no_idx)
                else:
                    set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "No")
                print("      ✅ set Insurance Vendor → No")
        except Exception as e:
            print(f"      ❌ update failed: {e}")

        print()
        time.sleep(1.0)

    print("Done ✅")


if __name__ == "__main__":
    main()