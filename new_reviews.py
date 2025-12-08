import os 
from dotenv import load_dotenv
load_dotenv()

# ======= CONFIG — EDIT THESE =======
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

ITEM_NAME_TITLE       = "Name"
HQ_ADDRESS_TITLE      = "HQ Address"
NEW_REVIEWS_30D_TITLE = "New Reviews (30 Days)"

HEADLESS   = True     # set False to watch the browser and debug
MAX_SCROLLS = 8
PAUSE       = 1.0
WAIT_SECS   = 6       # how long to wait for key elements
# ===================================

import json, re, time, requests
from typing import Dict, Any, List, Optional
from urllib.parse import quote_plus

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

RELATIVE_RE = re.compile(r"(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago", re.I)

# ---------------- Monday helpers ----------------
def gql(q: str, v: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=HEADERS, json={"query": q, "variables": v or {}}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def fetch_board_metadata(board_id: str):
    q = """
    query ($ids: [ID!]) {
      boards(ids: $ids) {
        name
        columns { id title type }
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
          items {
            id
            name
            column_values { id text value }
          }
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

def column_by_title(columns: List[Dict[str, Any]], title: str):
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None

def update_number(board_id: str, item_id: str, col_id: str, value: int) -> None:
    vals = json.dumps({col_id: str(value)})  # numbers expect string
    m = """
    mutation($board_id:ID!, $item_id:ID!, $vals: JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id }
    }"""
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": vals})

# ---------------- Selenium core ----------------
def make_driver(headless: bool = True):
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1300,1200"); opts.add_argument("--lang=en-US,en")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

def wait_for_any(driver, locators, timeout=WAIT_SECS):
    """Wait until any one of the given locators appears; return first we find or None."""
    end = time.time() + timeout
    while time.time() < end:
        for by, sel in locators:
            els = driver.find_elements(by, sel)
            if els:
                return els[0]
        time.sleep(0.3)
    return None

def maps_url_api_style(name: str, addr: str) -> str:
    q = f"{name} {addr}".strip()
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(q)}"

def maps_url_classic(name: str, addr: str) -> str:
    q = f"{name} {addr}".strip()
    return f"https://www.google.com/maps/search/{quote_plus(q)}"

def dismiss_consent(driver):
    for _ in range(2):
        btns = driver.find_elements(By.TAG_NAME, "button")
        clicked = False
        for b in btns:
            t = (b.text or "").strip().lower()
            if any(k in t for k in ["i agree", "accept", "got it", "agree"]):
                try:
                    b.click()
                    clicked = True
                    break
                except Exception:
                    pass
        if clicked:
            time.sleep(PAUSE)
            break

def open_first_result_if_list(driver) -> bool:
    """If we see a list, click the first result card."""
    # common: result cards have role='article'
    cards = driver.find_elements(By.XPATH, "//*[@role='article']")
    if cards:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cards[0])
            cards[0].click()
            time.sleep(PAUSE)
            return True
        except Exception:
            pass
    # fallback: try left panel links
    links = driver.find_elements(By.XPATH, "//a[@role='gridcell' or @role='link']")
    if links:
        try:
            links[0].click()
            time.sleep(PAUSE)
            return True
        except Exception:
            pass
    return False

def is_place_page(driver) -> bool:
    """Heuristic: place page shows a title and usually a Reviews button/link nearby."""
    # A place page often has a big title near the top and action buttons row.
    maybe_title = driver.find_elements(By.XPATH, "//h1 | //div[contains(@class,'fontHeadlineLarge')]")
    reviews_btn = driver.find_elements(By.XPATH, "//*[contains(text(),'Reviews') or contains(text(),'reviews')]")
    return bool(maybe_title) and bool(reviews_btn)

def open_reviews_panel(driver) -> None:
    # Try clicking "Reviews" buttons/links
    for _ in range(2):
        elems = driver.find_elements(By.XPATH, "//*[contains(text(),'Reviews') or contains(text(),'reviews')]")
        for e in elems:
            try:
                e.click()
                time.sleep(PAUSE)
                return
            except Exception:
                pass
        time.sleep(0.5)

def within_30_days(label: str) -> bool:
    s = (label or "").strip().lower()
    if s in ("a day ago", "a week ago", "a month ago"):
        s = s.replace("a ", "1 ")
    m = RELATIVE_RE.search(s)
    if not m:
        return False
    n = int(m.group(1)); unit = m.group(2)
    if unit.startswith("minute") or unit.startswith("hour"):
        return True
    if unit.startswith("day"):
        return n <= 30
    if unit.startswith("week"):
        return (n * 7) <= 30
    if unit.startswith("month"):
        return n <= 1
    return False

def count_new_reviews(driver) -> int:
    total_new = 0
    last_seen = -1
    for _ in range(MAX_SCROLLS):
        elems = driver.find_elements(By.XPATH, "//*[contains(text(),'ago') or contains(text(),'day') or contains(text(),'week') or contains(text(),'month')]")
        texts = [e.text.strip() for e in elems if e.text and len(e.text.strip()) <= 40]
        total_new = sum(1 for t in texts if within_30_days(t))

        # Scroll reviews panel (or page) for more
        try:
            panel = driver.find_element(By.XPATH, "//div[@role='region' or @aria-label='Reviews' or contains(@aria-label,'Reviews')]")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 800;", panel)
        except Exception:
            driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(PAUSE)

        if len(texts) == last_seen:
            break
        last_seen = len(texts)
    return total_new

def fetch_new_reviews_30d(driver, name: str, addr: str) -> int:
    """Try API-style link first; if not a place page, try classic; if list, click first result."""
    # Try API style search URL
    for url in (maps_url_api_style(name, addr), maps_url_classic(name, addr)):
        driver.get(url)
        time.sleep(PAUSE)
        dismiss_consent(driver)

        # Wait for either a place page hint (Reviews) or a results list
        wait_for_any(driver, [
            (By.XPATH, "//*[@role='article']"),                                 # list
            (By.XPATH, "//*[contains(text(),'Reviews') or contains(text(),'reviews')]")  # place page
        ], timeout=WAIT_SECS)

        # If it's a list, click first
        if not is_place_page(driver):
            opened = open_first_result_if_list(driver)
            if not opened:
                # try pressing Enter in case focus is on the first card
                try:
                    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ENTER)
                    time.sleep(PAUSE)
                except Exception:
                    pass

        # Now try to open Reviews
        open_reviews_panel(driver)

        # If we can see anything that looks like relative timestamps, count and return
        elems = driver.find_elements(By.XPATH, "//*[contains(text(),'ago') or contains(text(),'day') or contains(text(),'week') or contains(text(),'month')]")
        if elems:
            return count_new_reviews(driver)

    # If both flows fail, return 0 (no visible reviews found).
    return 0

# ---------------- Main ----------------
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Edit MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top of this file.")

    board_name, columns = fetch_board_metadata(MONDAY_BOARD_ID)
    print(f"[INFO] Board: {board_name}")

    col_name = column_by_title(columns, ITEM_NAME_TITLE)
    col_addr = column_by_title(columns, HQ_ADDRESS_TITLE)
    col_new  = column_by_title(columns, NEW_REVIEWS_30D_TITLE)

    if not col_name: raise SystemExit(f"Column '{ITEM_NAME_TITLE}' not found.")
    if not col_addr: raise SystemExit(f"Column '{HQ_ADDRESS_TITLE}' not found.")
    if not col_new:  raise SystemExit(f"Column '{NEW_REVIEWS_30D_TITLE}' not found.")

    name_col_id = col_name["id"]
    addr_col_id = col_addr["id"]
    new_col_id  = col_new["id"]

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items: {len(items)}")

    driver = make_driver(HEADLESS)
    try:
        for i, it in enumerate(items, 1):
            cv = {c["id"]: c for c in it.get("column_values", [])}
            name = cv.get(name_col_id, {}).get("text", "") or it["name"]
            addr = cv.get(addr_col_id, {}).get("text", "")

            if not name.strip():
                print(f"[{i}] ⚠️ Missing name; skipping")
                continue

            try:
                new30 = fetch_new_reviews_30d(driver, name, addr)
                update_number(MONDAY_BOARD_ID, it["id"], new_col_id, new30)
                print(f"[{i}] ✅ {name}: new reviews (≤30d) = {new30}")
            except Exception as e:
                print(f"[{i}] ❌ {name}: error → {e}")

            time.sleep(0.4)
    finally:
        driver.quit()

    print("Done.")

if __name__ == "__main__":
    main()