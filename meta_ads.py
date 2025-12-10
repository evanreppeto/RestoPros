import os 
from dotenv import load_dotenv
load_dotenv()
TARGET_ITEM_ID = os.getenv("TARGET_ITEM_ID")
# ======= CONFIG — EDIT THESE =======
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")


WEBSITE_TITLE         = "Website"            # Link/Text column with the website
META_ADS_STATUS_TITLE = "Meta Ads Active"    # Status column title
YES_LABEL             = "Yes"                # EXACT label in your Status column
NO_LABEL              = "No"                 # EXACT label in your Status column

HEADLESS   = True      # set False to watch
COUNTRY    = "US"      # Meta Ads Library country
WAIT_SECS  = 12
PAUSE      = 1.0
# ==============================

import re, json, time, requests, tldextract, idna
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, quote_plus

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

URL_IN_TEXT_RE = re.compile(r"https?://[^\s)>\]\"']+", re.I)

# --- Monday helpers ---
def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def fetch_board_metadata_with_settings(board_id: str):
    # We need settings_str to read Status labels
    q = """
    query ($ids: [ID!]) {
      boards(ids: $ids) {
        name
        columns { id title type settings_str }
      }
    }"""
    d = gql(q, {"ids":[board_id]})
    b = d["boards"][0]
    return b["name"], b["columns"]

def fetch_items(board_id: str) -> List[Dict[str, Any]]:
    q = """
    query ($board_id: [ID!], $cursor: String) {
      boards(ids: $board_id) {
        items_page(limit: 500, cursor: $cursor) {
          items { id name column_values { id text value } }
          cursor
        }
      }
    }"""
    items, cursor = [], None
    while True:
        d = gql(q, {"board_id":[board_id], "cursor":cursor})
        page = d["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor: break
    return items

def column_by_title(columns: List[Dict[str, Any]], title: str) -> Optional[Dict[str, Any]]:
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None

def change_values(board_id: str, item_id: str, values_dict: dict):
    m = """
    mutation($board_id: ID!, $item_id: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id }
    }"""
    return gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(values_dict)})

# --- Status label mapping ---
def parse_status_labels(settings_str: Optional[str]) -> Dict[str, int]:
    """
    Parse settings_str JSON for status labels -> index mapping.
    Returns dict like {"Working on it": 1, "Done": 2, ...} (label -> index).
    """
    if not settings_str:
        return {}
    try:
        s = json.loads(settings_str)
        labels = s.get("labels") or {}
        # labels is a dict of index->label strings, convert to label->index
        out = {}
        for idx_str, label in labels.items():
            try:
                idx = int(idx_str)
            except Exception:
                continue
            if isinstance(label, str) and label:
                out[label] = idx
        return out
    except Exception:
        return {}

# --- Column value helpers ---
def extract_url_from_cv(cv: Dict[str, Any], col_type: str) -> str:
    if not cv: return ""
    if col_type == "link":
        raw = cv.get("value")
        if raw:
            try:
                j = json.loads(raw)
                if j.get("url"): return j["url"].strip()
            except Exception:
                pass
        # fallback: parse from text
        t = (cv.get("text") or "")
        m = URL_IN_TEXT_RE.search(t)
        if m: return m.group(0).strip()
    else:
        t = (cv.get("text") or "").strip()
        if t: return t
    return ""

def registrable_domain(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u if u.startswith(("http://","https://")) else "https://" + u)
        host = (p.netloc or p.path).lower().split("@")[-1].split(":")[0].lstrip("www.")
        ext = tldextract.extract(host)
        d = f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else host
        try: d = idna.decode(d)
        except idna.IDNAError: pass
        return d
    except Exception:
        return ""

# --- Selenium / Meta Ads Library ---
def make_driver(headless: bool = True):
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,1100"); opts.add_argument("--lang=en-US,en")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

# Simple heuristics for the public library page
NO_ADS_PATTERNS = [
    re.compile(r"No ads to show", re.I),
    re.compile(r"No ads available", re.I),
    re.compile(r"We didn’t find any ads", re.I),
]
HAS_ADS_PATTERNS = [
    re.compile(r"results? for", re.I),   # "X results for ..."
    re.compile(r"Ad details", re.I),
    re.compile(r"Sponsored", re.I),
]

def meta_ads_active_for_query(driver, query: str, country: str = "US") -> bool:
    url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={quote_plus(country)}&q={quote_plus(query)}"
    driver.get(url)
    time.sleep(2.0)
    try:
        WebDriverWait(driver, WAIT_SECS).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "")
    except Exception:
        body_text = ""

    for pat in NO_ADS_PATTERNS:
        if pat.search(body_text):
            return False
    for pat in HAS_ADS_PATTERNS:
        if pat.search(body_text):
            return True

    # nudge load
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
        time.sleep(1.0)
        body_text2 = (driver.find_element(By.TAG_NAME, "body").text or "")
    except Exception:
        body_text2 = ""

    for pat in NO_ADS_PATTERNS:
        if pat.search(body_text2):
            return False
    for pat in HAS_ADS_PATTERNS:
        if pat.search(body_text2):
            return True

    return False

# --- Main ---
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Edit MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top of this file.")

    board_name, columns = fetch_board_metadata_with_settings(MONDAY_BOARD_ID)
    print(f"[INFO] Board: {board_name}")

    col_site = column_by_title(columns, WEBSITE_TITLE)
    if not col_site:
        raise SystemExit(f"Website column '{WEBSITE_TITLE}' not found.")
    col_status = column_by_title(columns, META_ADS_STATUS_TITLE)
    if not col_status:
        raise SystemExit(f"Status column '{META_ADS_STATUS_TITLE}' not found.")
    if (col_status.get("type") or "").lower() != "status":
        raise SystemExit(f"Column '{META_ADS_STATUS_TITLE}' is not a Status column.")

    # Build label->index map from settings_str
    label_to_index = parse_status_labels(col_status.get("settings_str"))
    if YES_LABEL not in label_to_index or NO_LABEL not in label_to_index:
        raise SystemExit(f"Status labels must include '{YES_LABEL}' and '{NO_LABEL}'. Found: {list(label_to_index.keys())}")

    yes_idx = label_to_index[YES_LABEL]
    no_idx  = label_to_index[NO_LABEL]

    site_col_id = col_site["id"]; site_col_type = col_site["type"]
    status_col_id = col_status["id"]

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
            cv_by_id = {c["id"]: c for c in it.get("column_values", [])}
            website_url = extract_url_from_cv(cv_by_id.get(site_col_id, {}), site_col_type)
            name        = it["name"]
            domain      = registrable_domain(website_url)

            # Current status text (to skip if already correct)
            current_text = (cv_by_id.get(status_col_id, {}) or {}).get("text") or ""
            current_text_norm = current_text.strip().lower()

            # Decide queries
            queries = []
            if domain:
                queries.append(domain)
                core = domain.split(".")[0]
                if core and core != domain:
                    queries.append(core)
            queries.append(name)

            active = False
            for q in queries:
                try:
                    active = meta_ads_active_for_query(driver, q, country=COUNTRY)
                    if active:
                        break
                except Exception:
                    pass
                time.sleep(0.4)

            desired_label = YES_LABEL if active else NO_LABEL
            desired_idx   = yes_idx if active else no_idx

            if current_text_norm == desired_label.lower():
                print(f"[{i}] ⏭️  {name} ({domain or 'no-site'}) already '{desired_label}' — skip")
                continue

            try:
                change_values(MONDAY_BOARD_ID, it["id"], {status_col_id: {"index": desired_idx}})
                print(f"[{i}] ✅ {name} ({domain or 'no-site'}) → Status '{desired_label}' (index {desired_idx})")
            except Exception as e:
                print(f"[{i}] ❌ {name} update failed: {e}")

            time.sleep(0.3)
    finally:
        driver.quit()

    print("\nDone.")

if __name__ == "__main__":
    main()