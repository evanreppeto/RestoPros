import os 
from dotenv import load_dotenv
load_dotenv()

# ======= CONFIG — EDIT THESE =======
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_TITLE     = "Website"          # column on Monday that holds the website (link or text)
LINKEDIN_TITLE    = "LinkedIn Active"  # Status column name on Monday
YES_LABEL         = "Yes"
NO_LABEL          = "No"

REQUEST_TIMEOUT   = 12
SLEEP_BETWEEN     = 1.0
# ==========================

import json, time, re, requests
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

URL_IN_TEXT_RE = re.compile(r"https?://[^\s)>\]\"']+", re.I)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


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


def column_by_title(columns: List[Dict[str, Any]], title: str) -> Optional[Dict[str, Any]]:
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None


def parse_status_labels(settings_str: Optional[str]) -> Dict[str, int]:
    if not settings_str:
        return {}
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


def change_values(board_id: str, item_id: str, values_dict: dict):
    m = """
    mutation($board_id: ID!, $item_id: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id }
    }"""
    gql(m, {"board_id": board_id, "item_id": item_id, "vals": json.dumps(values_dict)})


# ---------- website helpers ----------
def extract_url_from_cv(cv: Optional[Dict[str, Any]], col_type: str) -> str:
    """
    Monday "link" columns store value as JSON: {"url":"...","text":"..."}
    text columns just store text.
    """
    if not cv:
        return ""
    if col_type == "link":
        raw = cv.get("value")
        if raw:
            try:
                j = json.loads(raw)
                if j.get("url"):
                    return j["url"].strip()
            except Exception:
                pass
        text = (cv.get("text") or "").strip()
        m = URL_IN_TEXT_RE.search(text)
        if m:
            return m.group(0).strip()
        return ""
    else:
        return (cv.get("text") or "").strip()


def site_has_linkedin(url: str) -> bool:
    """
    Fetch the given URL and look for <a href="...linkedin.com/...">
    If the site's HTML contains such a link, return True.
    """
    if not url:
        return False

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        print(f"    ⚠️ could not fetch site: {e}")
        return False

    soup = BeautifulSoup(resp.text, "lxml")

    # check all anchor tags
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "linkedin.com" in href.lower():
            abs_href = urljoin(url, href)
            print(f"    🔗 found linkedin link: {abs_href}")
            return True

    # sometimes in meta tags
    for meta in soup.find_all("meta"):
        for v in meta.attrs.values():
            if isinstance(v, str) and "linkedin.com" in v.lower():
                print(f"    🔗 found linkedin in meta: {v}")
                return True

    return False


# ---------- main ----------
def main():
    if "PUT_YOUR_" in MONDAY_API_TOKEN or "PUT_YOUR_" in MONDAY_BOARD_ID:
        raise SystemExit("Edit MONDAY_API_TOKEN and MONDAY_BOARD_ID at the top of the script.")

    board = fetch_board_metadata_with_settings(MONDAY_BOARD_ID)
    board_name = board["name"]
    columns = board["columns"]
    print(f"[INFO] Board: {board_name}")

    col_site = column_by_title(columns, WEBSITE_TITLE)
    if not col_site:
        raise SystemExit(f"Website column '{WEBSITE_TITLE}' not found.")

    col_li = column_by_title(columns, LINKEDIN_TITLE)
    if not col_li:
        raise SystemExit(f"Status column '{LINKEDIN_TITLE}' not found.")
    if (col_li.get("type") or "").lower() != "status":
        raise SystemExit(f"Column '{LINKEDIN_TITLE}' must be a Status column.")

    label_map = parse_status_labels(col_li.get("settings_str"))
    if YES_LABEL not in label_map or NO_LABEL not in label_map:
        raise SystemExit(f"Status labels must include '{YES_LABEL}' and '{NO_LABEL}'. Found: {list(label_map.keys())}")
    yes_idx = label_map[YES_LABEL]
    no_idx  = label_map[NO_LABEL]

    site_col_id   = col_site["id"]
    site_col_type = col_site["type"]
    li_col_id     = col_li["id"]

    items = fetch_items(MONDAY_BOARD_ID)
    print(f"[INFO] Items to check: {len(items)}\n")

    for i, it in enumerate(items, 1):
        item_id = it["id"]
        name    = it["name"]
        cvmap   = {cv["id"]: cv for cv in it.get("column_values", [])}

        current_text = (cvmap.get(li_col_id, {}) or {}).get("text") or ""
        current_norm = current_text.strip().lower()

        website_url = extract_url_from_cv(cvmap.get(site_col_id), site_col_type)
        print(f"[{i}] {name} — site: {website_url or '(none)'}")

        has_li = False
        if website_url:
            has_li = site_has_linkedin(website_url)

        desired_label = YES_LABEL if has_li else NO_LABEL
        desired_idx   = yes_idx if has_li else no_idx

        # skip if already correct
        if current_norm == desired_label.lower():
            print(f"    ⏭️ already '{desired_label}', skip\n")
            continue

        # update monday
        try:
            change_values(MONDAY_BOARD_ID, item_id, {li_col_id: {"index": desired_idx}})
            print(f"    ✅ set to '{desired_label}'\n")
        except Exception as e:
            print(f"    ❌ update failed: {e}\n")

        time.sleep(SLEEP_BETWEEN)

    print("Done ✅")


if __name__ == "__main__":
    main()