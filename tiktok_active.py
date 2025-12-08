import os 
from dotenv import load_dotenv
load_dotenv()

# ======= CONFIG — EDIT THESE =======

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_TITLE     = "Website"
TIKTOK_STATUS_COL = "TikTok Active"
YES_LABEL         = "Yes"
NO_LABEL          = "No"

HEADLESS = False   # keep False so you can watch
PAUSE    = 1.0
# ========================

import json, time, re, requests, tldextract, idna
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
MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

URL_IN_TEXT_RE = re.compile(r"https?://[^\s)>\]\"']+", re.I)

# ---------- Monday helpers ----------
def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=MONDAY_HEADERS, json={"query": query, "variables": variables or {}}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def fetch_board_metadata_with_settings(board_id: str):
    q = """
    query ($ids: [ID!]) {
      boards(ids: $ids) {
        name
        columns { id title type settings_str }
      }
    }"""
    d = gql(q, {"ids":[board_id]})
    return d["boards"][0]

def fetch_items(board_id: str) -> List[Dict[str, Any]]:
    q = """
    query ($board_id:[ID!], $cursor:String) {
      boards(ids:$board_id) {
        items_page(limit:500, cursor:$cursor) {
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
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(values_dict)})

def parse_status_labels(settings_str: Optional[str]) -> Dict[str, int]:
    if not settings_str: return {}
    try:
        s = json.loads(settings_str)
        labels = s.get("labels") or {}
        out = {}
        for idx_str, label in labels.items():
            try:
                idx = int(idx_str)
            except Exception:
                continue
            if label:
                out[label] = idx
        return out
    except Exception:
        return {}

# ---------- URL helpers ----------
def extract_url_from_cv(cv: Optional[Dict[str, Any]], col_type: str) -> str:
    if not cv: return ""
    if col_type == "link":
        raw = cv.get("value")
        if raw:
            try:
                j = json.loads(raw)
                if j.get("url"): return j["url"].strip()
            except Exception:
                pass
        text = (cv.get("text") or "").strip()
        m = URL_IN_TEXT_RE.search(text)
        if m: return m.group(0).strip()
        return ""
    else:
        return (cv.get("text") or "").strip()

def registrable_domain(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u if u.startswith(("http://","https://")) else "https://"+u)
        host = (p.netloc or p.path).lower().split("@")[-1].split(":")[0].lstrip("www.")
        ext = tldextract.extract(host)
        d = f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else host
        try: d = idna.decode(d)
        except idna.IDNAError: pass
        return d
    except Exception:
        return ""

# ---------- TikTok helpers ----------
def make_driver(headless: bool = False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,1000")
    # make it a bit more like a real browser
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

# possible "not found" texts on TikTok
NOT_FOUND_PATTERNS = [
    re.compile(r"Couldn't find this account", re.I),
    re.compile(r"couldn.t find this account", re.I),
    re.compile(r"page not found", re.I),
    re.compile(r"this account is private", re.I),  # private is still "exists", so handle carefully
    re.compile(r"login", re.I),
]

# possible "real profile" hints
PROFILE_PATTERNS = [
    re.compile(r"Followers", re.I),
    re.compile(r"Following", re.I),
    re.compile(r"Likes", re.I),
    re.compile(r"@[^ \n]+", re.I),
]

def click_tiktok_consent(driver):
    """Try to close cookie / consent popups so we can read the page."""
    time.sleep(1)
    candidates = [
        "//button[contains(.,'Accept all')]",
        "//button[contains(.,'Accept')]",
        "//button[contains(.,'OK')]",
        "//button[contains(.,'I agree')]",
    ]
    for xp in candidates:
        try:
            btn = driver.find_element(By.XPATH, xp)
            if btn.is_displayed():
                btn.click()
                time.sleep(0.5)
                break
        except Exception:
            pass

def looks_like_tiktok_profile(driver) -> bool:
    """Heuristic: look at page text for follow/follower UI."""
    try:
        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception:
        pass
    time.sleep(1.0)
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text = ""
    # if "Couldn't find this account" we fail
    for pat in NOT_FOUND_PATTERNS:
        if pat.search(body_text):
            # EXCEPTION: if it's private, that actually means the account exists
            if "private" in pat.pattern.lower():
                return True
            return False
    # if we see followers/following it's good
    for pat in PROFILE_PATTERNS:
        if pat.search(body_text):
            return True
    # sometimes title shows the handle
    try:
        title = driver.title or ""
        if "TikTok" in title and "@" in title:
            return True
    except Exception:
        pass
    return False

def make_tiktok_candidates(name: str, domain: str) -> List[str]:
    """
    Generate a list of possible TikTok profile URLs for this company.
    We try:
      @name-no-spaces-lower
      @name_with_underscores
      @domain-core
    """
    candidates = []

    def slugify(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^\w]+", "", s)  # remove spaces and punctuation
        return s

    name_slug = slugify(name)
    if name_slug:
        candidates.append(f"https://www.tiktok.com/@{name_slug}")

    # version with underscore
    name_us = re.sub(r"[^\w]+", "_", name.strip().lower())
    if name_us and name_us != name_slug:
        candidates.append(f"https://www.tiktok.com/@{name_us}")

    if domain:
        core = domain.split(".")[0]
        core_slug = slugify(core)
        if core_slug and f"https://www.tiktok.com/@{core_slug}" not in candidates:
            candidates.append(f"https://www.tiktok.com/@{core_slug}")

    # ensure unique in order
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq

# ---------- main ----------
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Edit MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top.")

    board = fetch_board_metadata_with_settings(MONDAY_BOARD_ID)
    board_name = board["name"]
    columns = board["columns"]
    print(f"[INFO] Board: {board_name}")

    col_site = column_by_title(columns, WEBSITE_TITLE)
    if not col_site:
        raise SystemExit(f"Website column '{WEBSITE_TITLE}' not found.")

    col_tt = column_by_title(columns, TIKTOK_STATUS_COL)
    if not col_tt:
        raise SystemExit(f"Status column '{TIKTOK_STATUS_COL}' not found.")
    if (col_tt.get("type") or "").lower() != "status":
        raise SystemExit(f"Column '{TIKTOK_STATUS_COL}' must be a Status column.")

    label_map = parse_status_labels(col_tt.get("settings_str"))
    if YES_LABEL not in label_map or NO_LABEL not in label_map:
        raise SystemExit(f"Status labels must include '{YES_LABEL}' and '{NO_LABEL}'. Found: {list(label_map.keys())}")
    yes_idx, no_idx = label_map[YES_LABEL], label_map[NO_LABEL]

    site_col_id   = col_site["id"]
    site_col_type = col_site["type"]
    tt_col_id     = col_tt["id"]

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items to check: {len(items)}")

    driver = make_driver(HEADLESS)
    try:
        for i, it in enumerate(items, 1):
            name = it["name"]
            cvmap = {cv["id"]: cv for cv in it.get("column_values", [])}
            current_text = (cvmap.get(tt_col_id, {}) or {}).get("text") or ""
            current_norm = current_text.strip().lower()

            # skip if already correct? (optional — keeps idempotent)
            # if current_norm in (YES_LABEL.lower(), NO_LABEL.lower()):
            #     print(f"[{i}] ⏭️ {name} already set to '{current_text}'")
            #     continue

            website_url = extract_url_from_cv(cvmap.get(site_col_id), site_col_type)
            domain = registrable_domain(website_url)

            candidates = make_tiktok_candidates(name, domain)
            print(f"\n[{i}] {name} — candidates:")
            for c in candidates:
                print(f"   - {c}")

            found = False
            for url in candidates:
                print(f"   → trying {url}")
                try:
                    driver.get(url)
                    # allow page to load
                    click_tiktok_consent(driver)
                    time.sleep(1.3)
                    if looks_like_tiktok_profile(driver):
                        print(f"   ✅ looks like a TikTok profile: {url}")
                        found = True
                        break
                    else:
                        print(f"   ❌ not a profile (or not found)")
                except Exception as e:
                    print(f"   ⚠️ error loading {url}: {e}")
                time.sleep(PAUSE)

            desired_label = YES_LABEL if found else NO_LABEL
            desired_idx   = yes_idx if found else no_idx

            # only update if different
            if current_norm == desired_label.lower():
                print(f"   ⏭️ already '{desired_label}' — skipping Monday update")
            else:
                try:
                    change_values(MONDAY_BOARD_ID, it["id"], {tt_col_id: {"index": desired_idx}})
                    print(f"   ✅ Monday updated → '{desired_label}'")
                except Exception as e:
                    print(f"   ❌ Monday update failed: {e}")

            time.sleep(PAUSE)

    finally:
        driver.quit()

    print("\nDone.")

if __name__ == "__main__":
    main()