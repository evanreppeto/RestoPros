import os 
from dotenv import load_dotenv
load_dotenv()
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")

# ======= CONFIG — EDIT THESE =======
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")
WEBSITE_TITLE    = "Website"              # Column title that holds the website link
STATUS_TITLE     = "Google Ads Active"    # Status column to write "Yes"/"No"
HEADLESS         = False                  # True to run without UI
# Optional hard overrides if a brand must use a specific domain:
DOMAIN_OVERRIDE = {
    # "Romexterra Construction Fire and Water Restoration Services of Chicago": "romexterrarestoration.com",
}
# ======================

import json, re, time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import tldextract

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

ADS_RE = re.compile(r"(\d{1,3}(?:,\d{3})*)\s+ads\b", re.I)


# ---------- Monday helpers ----------
def gql(q: str, v: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=HEADERS, json={"query": q, "variables": v or {}}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def fetch_board(board_id: str) -> Dict[str, Any]:
    q = """query($ids:[ID!]){ boards(ids:$ids){ name columns{ id title type settings_str } } }"""
    return gql(q, {"ids":[board_id]})["boards"][0]

def fetch_items(board_id: str) -> List[Dict[str, Any]]:
    q = """
    query($board_id:[ID!],$cursor:String){
      boards(ids:$board_id){
        items_page(limit:500,cursor:$cursor){
          items{ id name column_values{ id text value } }
          cursor
        }
      }
    }"""
    items, cursor = [], None
    while True:
        d = gql(q, {"board_id":[board_id], "cursor":cursor})
        p = d["boards"][0]["items_page"]
        items.extend(p["items"])
        cursor = p.get("cursor")
        if not cursor: break
    return items

def col_by_title(columns: List[Dict[str, Any]], title: str) -> Optional[Dict[str, Any]]:
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None

def status_label_indices(settings_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not settings_str: return (None, None)
    try:
        s = json.loads(settings_str)
        labels = s.get("labels") or {}
        name_to_idx = { (v or "").strip().lower(): int(k) for k, v in labels.items() if isinstance(v, str) }
        yes_idx = next((name_to_idx[k] for k in ("yes","true","active") if k in name_to_idx), None)
        no_idx  = next((name_to_idx[k] for k in ("no","false","inactive") if k in name_to_idx), None)
        return (yes_idx, no_idx)
    except Exception:
        return (None, None)

def set_status_by_label(board_id: str, item_id: str, column_id: str, label_text: str) -> None:
    vals = {column_id: {"label": label_text}}
    m = """mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id } }"""
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})

def set_status_by_index(board_id: str, item_id: str, column_id: str, index: int) -> None:
    vals = {column_id: {"index": index}}
    m = """mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id } }"""
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})


# ---------- STRICT website parsing ----------
def link_url_from_cv(cv: Optional[Dict[str, Any]], col_type: str) -> str:
    """
    STRICT: Only take the URL from column_value.value.url when type == 'link'.
    Fallback to text ONLY if value.url missing.
    Never strip/guess from arbitrary display text like "Website - https://...".
    """
    if not cv:
        return ""
    if (col_type or "").lower() == "link":
        raw = cv.get("value")
        if raw:
            try:
                j = json.loads(raw)
                u = (j.get("url") or "").strip()
                if u:
                    return u
            except Exception:
                pass
        # last resort: text (some boards store just the URL in text)
        return (cv.get("text") or "").strip()
    # if the column is not a link, use whatever text is stored
    return (cv.get("text") or "").strip()

def registrable_domain_from_url(u: str) -> str:
    """
    Trim to eTLD+1 (registrable domain) from a full URL or hostname.
    Examples:
      https://www.romexterrarestoration.com/      -> romexterrarestoration.com
      https://restoreconstruction.com/location/... -> restoreconstruction.com
      romexterrarestoration.com                    -> romexterrarestoration.com
    """
    s = (u or "").strip()
    if not s:
        return ""
    if not s.startswith(("http://","https://")):
        s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or p.path).lower()
        host = host.split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        ext = tldextract.extract(host)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
        return host
    except Exception:
        return ""


# ---------- Selenium ----------
def make_driver(headless=False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1300,1000")
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })
    return driver

def get_ads_count(driver, domain: str) -> Optional[int]:
    url = f"https://adstransparency.google.com/?region=anywhere&domain={domain}"
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception:
        return None
    time.sleep(1.2)
    soup = BeautifulSoup(driver.page_source or "", "lxml")
    txt  = soup.get_text(" ", strip=True)
    m = ADS_RE.search(txt)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


# ---------- Main ----------
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Set MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top.")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]
    print(f"[INFO] Board: {board['name']}")

    site_col = col_by_title(cols, WEBSITE_TITLE)
    if not site_col:
        raise SystemExit(f"Column '{WEBSITE_TITLE}' not found.")

    status_col = col_by_title(cols, STATUS_TITLE)
    if not status_col:
        raise SystemExit(f"Status column '{STATUS_TITLE}' not found.")
    status_type = (status_col.get("type") or "").lower()
    if status_type not in ("color","status"):
        raise SystemExit(f"Column '{STATUS_TITLE}' must be a Status type (currently '{status_type}').")

    yes_idx, no_idx = status_label_indices(status_col.get("settings_str"))

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items: {len(items)}\n")

    if TARGET_ITEM_ID:
        filtered = [it for it in items if str(it["id"]) == str(TARGET_ITEM_ID)]
        print(f"[INFO] Filtered to TARGET_ITEM_ID={TARGET_ITEM_ID} → {len(filtered)} matching item(s)")
        items = filtered
        if not items:
            print("[INFO] No matching item for TARGET_ITEM_ID; nothing to do.")
            return

    driver = make_driver(HEADLESS)
    try:
        for i, it in enumerate(items, 1):
            name = it["name"]
            cv_map = {c["id"]: c for c in it["column_values"]}
            existing_text = (cv_map.get(status_col["id"], {}) or {}).get("text") or ""
            existing_lc   = existing_text.strip().lower()

            # STRICT: read exact link from value.url
            raw_from_board = link_url_from_cv(cv_map.get(site_col["id"]), site_col["type"])
            # Allow per-item override if provided
            domain = DOMAIN_OVERRIDE.get(name) or registrable_domain_from_url(raw_from_board)

            print(f"[{i}] {name}")
            print(f"    raw_url_from_board: {raw_from_board!r}")
            print(f"    parsed_domain     : {domain!r}")

            if not domain:
                desired = "no"
                if existing_lc != desired:
                    try:
                        if no_idx is not None:
                            set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], no_idx)
                        else:
                            set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "No")
                        print(f"    → No (no valid domain)")
                    except Exception as e:
                        print(f"    update failed: {e}")
                else:
                    print("    already No; skip")
                continue

            atc_url = f"https://adstransparency.google.com/?region=anywhere&domain={domain}"
            print(f"    visiting          : {atc_url}")

            count = get_ads_count(driver, domain)
            if count is None:
                desired = "no"
                if existing_lc != desired:
                    try:
                        if no_idx is not None:
                            set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], no_idx)
                        else:
                            set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "No")
                        print("    ❔ 'X ads' not found → set No")
                    except Exception as e:
                        print(f"    update failed: {e}")
                else:
                    print("    'X ads' not found → already No; skip")
                continue

            desired = "yes" if count > 0 else "no"
            if existing_lc == desired:
                print(f"    {count} ads → already {desired.capitalize()}; skip")
                continue

            try:
                if desired == "yes":
                    if yes_idx is not None:
                        set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], yes_idx)
                    else:
                        set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "Yes")
                    print(f"    ✅ {count} ads → set Yes")
                else:
                    if no_idx is not None:
                        set_status_by_index(MONDAY_BOARD_ID, it["id"], status_col["id"], no_idx)
                    else:
                        set_status_by_label(MONDAY_BOARD_ID, it["id"], status_col["id"], "No")
                    print(f"    0 ads → set No")
            except Exception as e:
                print(f"    update failed: {e}")

            time.sleep(0.4)
    finally:
        driver.quit()

    print("\nDone ✅")

if __name__ == "__main__":
    main()