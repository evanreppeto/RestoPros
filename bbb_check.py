import os 
from dotenv import load_dotenv
load_dotenv()

# ======= CONFIG — EDIT THESE =======
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")
LOCATION_QUERY   = "Chicago, IL"
HEADLESS         = False
PREFERRED_TITLE  = "BBB Accreditation"  # We'll find/ create if missing
CREATE_IF_MISSING = True
# ========================

import json, time, re, requests, difflib
from typing import Dict, Any, List, Optional, Tuple
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

API_URL = "https://api.monday.com/v2"
HEADERS_MONDAY = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

ACCRED_POS_RE = re.compile(r"\bBBB\s+Accredited\s+Business\b", re.I)
ACCRED_NEG_RE = re.compile(r"\b(Not\s+BBB\s+accredited|This\s+business\s+is\s+not\s+BBB\s+accredited)\b", re.I)

# ---------- Monday helpers ----------
def gql(q: str, v: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=HEADERS_MONDAY, json={"query": q, "variables": v or {}}, timeout=30)
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

def create_status_column(board_id: str, title: str) -> Dict[str, Any]:
    m = """
    mutation($board_id:ID!, $title:String!, $col_type:ColumnType!){
      create_column(board_id:$board_id, title:$title, column_type:$col_type){
        id title type settings_str
      }
    }"""
    col = gql(m, {"board_id": board_id, "title": title, "col_type": "status"})["create_column"]
    # Try to seed Yes/No labels (optional)
    try:
        settings = {"labels": {"1": "Yes", "2": "No"}}
        m2 = """mutation($board_id:ID!, $column_id:String!, $settings:String!){
          change_column_settings(board_id:$board_id, column_id:$column_id, settings:$settings){ id } }"""
        gql(m2, {"board_id":board_id, "column_id":col["id"], "settings":json.dumps(settings)})
    except Exception:
        pass
    return col

def fuzzy_find_bbb_column(cols: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for c in cols:
        if c["title"].strip().lower() == PREFERRED_TITLE.lower():
            return c
    for c in cols:
        if "bbb" in c["title"].strip().lower():
            return c
    return None

def extract_yes_no_indices(settings_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not settings_str: return (None, None)
    try:
        s = json.loads(settings_str)
    except Exception:
        return (None, None)
    labels = s.get("labels") or {}
    names = { (v or "").strip().lower(): int(k) for k, v in labels.items() if isinstance(v, str) }
    yes_idx = next((idx for name, idx in names.items() if name in ("yes","true","active")), None)
    no_idx  = next((idx for name, idx in names.items() if name in ("no","false","inactive")), None)
    return (yes_idx, no_idx)

def update_bbb_generic(board_id: str, item_id: str, col: Dict[str, Any], value_bool: bool):
    col_id = col["id"]; ctype = (col.get("type") or "").lower()
    if ctype == "color":  # Status
        yes_idx, no_idx = extract_yes_no_indices(col.get("settings_str"))
        if yes_idx is not None and no_idx is not None:
            vals = {col_id: {"index": (yes_idx if value_bool else no_idx)}}
        else:
            vals = {col_id: {"label": "Yes" if value_bool else "No"}}
    elif ctype == "dropdown":
        vals = {col_id: {"labels": ["Yes" if value_bool else "No"]}}
    elif ctype == "checkbox":
        vals = {col_id: {"checked": "true" if value_bool else "false"}}
    elif ctype in ("text", "long-text"):
        vals = {col_id: "Yes" if value_bool else "No"}
    else:
        vals = {col_id: "Yes" if value_bool else "No"}

    m = """mutation($board_id:ID!, $item_id:ID!, $vals:JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id } }"""
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(vals)})

# ---------- Selenium setup ----------
def make_driver(headless: bool = False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1300,1000")
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

# ---------- BBB search & selection ----------
def open_bbb_results(driver, company: str, location: str) -> bool:
    # Use direct search URL (more stable than typing on homepage)
    url = f"https://www.bbb.org/search?find_text={requests.utils.quote(company)}&find_loc={requests.utils.quote(location)}"
    driver.get(url)
    try:
        WebDriverWait(driver, 12).until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'/profile/') or contains(@href,'/business-reviews/')]")
        ))
        time.sleep(0.3)
        return True
    except Exception:
        # Sometimes it lands directly on a profile
        return ("bbb.org/profile" in driver.current_url) or ("bbb.org/business-reviews" in driver.current_url)

def collect_results(driver) -> List[Dict[str, str]]:
    """
    Return a list of dicts: {href, name, location, text}
    We grab anchors that look like business cards and their nearby name/location texts.
    """
    soup = BeautifulSoup(driver.page_source or "", "lxml")
    results: List[Dict[str, str]] = []

    # Candidate links
    for a in soup.select("a[href*='/profile/'], a[href*='/business-reviews/']"):
        href = a.get("href") or ""
        if not href.startswith("http"):
            href = "https://www.bbb.org" + href

        # Try to get a nearby card container text
        card = a.find_parent(["article", "div", "li"]) or a.parent
        text = card.get_text(separator=" ", strip=True) if card else a.get_text(" ", strip=True)

        # Name heuristic: link text if looks like a business name, else first bold/header nearby
        name = a.get_text(" ", strip=True)
        if not name or len(name) < 2:
            h = card.select_one("h2, h3, strong") if card else None
            if h: name = h.get_text(" ", strip=True)

        # Location heuristic: look for “Chicago” line in the card text
        loc = ""
        m = re.search(r"\bChicago\b.*", text, flags=re.I)
        if m: loc = m.group(0)[:120]

        results.append({"href": href, "name": name, "location": loc, "text": text})

    # Deduplicate by href
    seen = set()
    deduped = []
    for r in results:
        if r["href"] in seen: continue
        seen.add(r["href"]); deduped.append(r)

    return deduped

def score_result(r: Dict[str, str], target_name: str, want_city: str) -> float:
    """
    Higher is better.
    - Name similarity (difflib ratio) weighted heavily
    - +0.2 bonus if location mentions desired city token
    - Penalize clear “advertising” hubs (edge cases)
    """
    name = (r["name"] or "").lower()
    t    = (target_name or "").lower()
    sim  = difflib.SequenceMatcher(None, name, t).ratio()  # 0..1
    score = sim

    if want_city:
        city_token = want_city.split(",")[0].strip().lower()
        if city_token and (city_token in (r["location"] or "").lower() or city_token in (r["text"] or "").lower()):
            score += 0.2

    # Downweight obvious non-business pages (rare)
    if "/claim-your-business" in r["href"]:
        score -= 0.2

    return score

def pick_best_result(driver, company: str, location: str) -> Optional[str]:
    results = collect_results(driver)
    if not results:
        # Maybe already on profile
        if ("bbb.org/profile" in driver.current_url) or ("bbb.org/business-reviews" in driver.current_url):
            return driver.current_url
        return None

    scored = [(score_result(r, company, location), r) for r in results]
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best = scored[0]

    # Optional: require a minimum confidence
    if best_score < 0.45:
        print(f"    ⚠️ Low confidence pick: {best['name']!r} (score={best_score:.2f})")
    else:
        print(f"    ✓ Picked: {best['name']!r} (score={best_score:.2f})")

    return best["href"]

def go_to_url(driver, url: str) -> bool:
    try:
        driver.get(url)
        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        return True
    except Exception:
        return False

def detect_accreditation(driver) -> Optional[bool]:
    try:
        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.2)
    except Exception:
        pass
    html = driver.page_source or ""
    if ACCRED_POS_RE.search(html): return True
    if ACCRED_NEG_RE.search(html): return False
    # Heuristics: alt/aria labels
    try:
        soup = BeautifulSoup(html, "lxml")
        for img in soup.find_all("img", alt=True):
            if ACCRED_POS_RE.search(img.get("alt","")):
                return True
        for tag in soup.find_all(True, attrs={"aria-label": True}):
            if ACCRED_POS_RE.search(tag.get("aria-label","")):
                return True
    except Exception:
        pass
    return None

# ---------- main ----------
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Set MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top.")

    board = fetch_board(MONDAY_BOARD_ID)
    cols  = board["columns"]
    print(f"[INFO] Board: {board['name']}")

    bbb_col = fuzzy_find_bbb_column(cols)
    if not bbb_col:
        if not CREATE_IF_MISSING:
            raise SystemExit("No column with 'bbb' in the title found. Rename/create one or set CREATE_IF_MISSING=True.")
        print(f"[INFO] Creating Status column '{PREFERRED_TITLE}'…")
        bbb_col = create_status_column(MONDAY_BOARD_ID, PREFERRED_TITLE)
    print(f"[INFO] BBB column → title='{bbb_col['title']}', type='{bbb_col['type']}', id='{bbb_col['id']}'")

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items: {len(items)}\n")

    driver = make_driver(HEADLESS)
    try:
        for i, it in enumerate(items, 1):
            name = it["name"]
            cv   = {c["id"]: c for c in it["column_values"]}
            existing = (cv.get(bbb_col["id"], {}) or {}).get("text") or ""
            if existing.strip():
                print(f"[{i}] ⏭️ {name}: already '{existing}'")
                continue

            print(f"[{i}] BBB search → {name} | {LOCATION_QUERY}")
            if not open_bbb_results(driver, name, LOCATION_QUERY):
                print("    ❌ Could not load results/profile")
                continue

            target_url = pick_best_result(driver, name, LOCATION_QUERY)
            if not target_url:
                print("    ❌ No suitable result")
                continue

            if not go_to_url(driver, target_url):
                print("    ❌ Failed to open selected profile")
                continue

            acc = detect_accreditation(driver)
            if acc is None:
                print("    ❔ No clear accreditation signal")
                continue

            try:
                update_bbb_generic(MONDAY_BOARD_ID, it["id"], bbb_col, acc)
                print(f"    ✅ Updated '{bbb_col['title']}' → {'Yes' if acc else 'No'}")
            except Exception as e:
                print(f"    ❌ Monday update failed: {e}")

            time.sleep(0.5)
    finally:
        driver.quit()

    print("\nDone ✅")

if __name__ == "__main__":
    main()