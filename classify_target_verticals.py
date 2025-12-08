#!/usr/bin/env python3
"""
classify_target_verticals.py (evidence-based)

Enhancements:
- Tracks evidence (matched phrases and URLs) for each label.
- Any explicit 'residential' (or homeowner synonyms) will include Residential.
- Weights matches found on high-signal pages (/residential, /services, /industries).
- Prints reasons for each applied label; toggle verbosity via DEBUG_EVIDENCE.

Env:
  MONDAY_API_TOKEN, MONDAY_BOARD_ID
  WEBSITE_COLUMN_TITLE=Website
  TARGET_VERTICALS_COLUMN_TITLE=Target Verticals
  ITEM_NAME_COLUMN_TITLE=Name
  DEBUG_EVIDENCE=true|false
"""

import os, re, json, time, idna, tldextract, requests
from typing import Dict, Any, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ---------------- Config ----------------
load_dotenv()
API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID  = os.getenv("MONDAY_BOARD_ID")

WEBSITE_COL_TITLE          = os.getenv("WEBSITE_COLUMN_TITLE", "Website").strip()
TARGET_DROPDOWN_COL_TITLE  = os.getenv("TARGET_VERTICALS_COLUMN_TITLE", "Target Verticals").strip()
ITEM_NAME_COL_TITLE        = os.getenv("ITEM_NAME_COLUMN_TITLE", "Name").strip()

DEBUG_EVIDENCE = os.getenv("DEBUG_EVIDENCE", "false").lower() == "true"

API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

USER_AGENT = "Mozilla/5.0 (compatible; TargetVerticalsBot/1.1; +https://example.com/bot)"
REQ_HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 20

DEFAULT_PATHS = ["/", "/services", "/industries", "/residential", "/commercial", "/insurance"]
MAX_INTERNAL_LINKS_PER_SITE = 8
SLEEP_BETWEEN_FETCHES = 0.2
SLEEP_BETWEEN_COMPANIES = 0.4

# ---------------- Monday GraphQL ----------------
def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    r = requests.post(API_URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=60)
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
        columns { id title type settings_str }
      }
    }"""
    d = gql(q, {"ids": [board_id]})
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
        d = gql(q, {"board_id": [board_id], "cursor": cursor})
        page = d["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor:
            break
    return items

def update_dropdown_labels(board_id: str, item_id: str, column_id: str, labels: List[str]) -> None:
    mutation = """
    mutation($board_id: ID!, $item_id: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $vals) { id }
    }"""
    vals = json.dumps({column_id: {"labels": labels}})
    gql(mutation, {"board_id": board_id, "item_id": item_id, "vals": vals})

# ---------------- Column helpers ----------------
def resolve_column(columns: List[Dict[str, Any]], title: str) -> Optional[Dict[str, Any]]:
    if not title: return None
    for c in columns:
        if c["title"].strip().lower() == title.strip().lower():
            return c
    return None

def dropdown_label_names(columns: List[Dict[str, Any]], title: str) -> List[str]:
    col = resolve_column(columns, title)
    if not col or col["type"] != "dropdown":
        return []
    try:
        settings = json.loads(col.get("settings_str") or "{}")
        raw = settings.get("labels", [])
        out = []
        for x in raw:
            if isinstance(x, dict) and "name" in x:
                out.append(x["name"])
            elif isinstance(x, str):
                out.append(x)
        return out
    except Exception:
        return []

# ---------------- Website extraction (Link/Text safe) ----------------
URL_IN_TEXT_RE = re.compile(r"https?://[^\s)>\]\"']+", re.I)

def extract_website_from_item(item: dict, website_col_id: str, website_col_type: str) -> str:
    cv = next((c for c in item.get("column_values", []) if c["id"] == website_col_id), None)
    if cv:
        if website_col_type == "link":
            raw = cv.get("value")
            if raw:
                try:
                    j = json.loads(raw)
                    if j.get("url"):
                        return j["url"].strip()
                except Exception:
                    pass
            t = (cv.get("text") or "")
            m = URL_IN_TEXT_RE.search(t)
            if m:
                return m.group(0).strip()
        else:
            t = (cv.get("text") or "").strip()
            if t:
                return t
    for c in item.get("column_values", []):
        t = (c.get("text") or "")
        m = URL_IN_TEXT_RE.search(t)
        if m:
            return m.group(0).strip()
    return ""

def sanitize_url_for_home(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    try:
        p = urlparse(u)
        host = (p.netloc or p.path).split("@")[-1].split(":")[0]
        if not host:
            return ""
        return f"{p.scheme}://{host}"
    except Exception:
        return ""

# ---------------- Fetch & parse pages ----------------
def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=REQ_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        if "text/html" not in (r.headers.get("Content-Type","").lower()):
            return None
        return r.text
    except Exception:
        return None

def page_text(url: str) -> str:
    html = fetch_html(url)
    if not html: return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script","style","noscript"]):
        tag.decompose()
    bits = [t.get_text(" ", strip=True) for t in soup.find_all(["h1","h2","h3","p","li","a","span","strong","em"])]
    return " ".join(bits)

def discover_extra_links(base: str, html: str) -> List[str]:
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        abs_url = urljoin(base, href)
        try:
            b = urlparse(base).netloc
            h = urlparse(abs_url).netloc
            same = (h.lower().lstrip("www.") == b.lower().lstrip("www."))
        except Exception:
            same = False
        if not same:
            continue
        text = (a.get_text(" ", strip=True) or "").lower()
        h = abs_url.lower()
        if any(k in h or k in text for k in ["residential","commercial","insurance","industries","sectors","markets"]):
            if abs_url not in seen:
                out.append(abs_url); seen.add(abs_url)
                if len(out) >= MAX_INTERNAL_LINKS_PER_SITE:
                    break
    return out

# ---------------- Evidence-based classification ----------------
# phrase → weight (weights are modest; single explicit keyword still triggers inclusion)
RESI_PHRASES = {
    r"\bresidential\b": 2.0,
    r"\bhomeowner(s)?\b": 1.5,
    r"\bhome services?\b": 1.2,
    r"\bhousehold\b": 1.0,
    r"\bmulti-?family\b": 1.3,
    r"\bcondo(s)?\b": 1.0,
    r"\bapartment(s)?\b": 1.0,
    r"\bHOA\b": 1.2,
}
COMM_PHRASES = {
    r"\bcommercial\b": 2.0,
    r"\bb2b\b": 1.2,
    r"\boffice(s)?\b": 1.0,
    r"\bretail\b": 1.0,
    r"\bindustr(y|ies)\b": 1.2,
    r"\bindustrial\b": 1.2,
    r"\bproperty management\b": 1.5,
    r"\bfacilit(y|ies) management\b": 1.5,
    r"\benterprise\b": 1.2,
}
INS_PHRASES = {
    r"\binsurance\b": 2.0,
    r"\bcarrier(s)?\b": 1.5,
    r"\bTPA\b": 1.5,
    r"\bthird[- ]party administrator(s)?\b": 1.5,
    r"\bprogram work\b": 1.2,
    r"\bpreferred vendor\b": 1.2,
    r"\bclaims?\b": 1.2,
    r"\bCAT\b": 1.0,
    r"\bXactimate\b": 1.2,
}

# boost by URL path hints
URL_BOOSTS = [
    (re.compile(r"/residential", re.I), 1.5),
    (re.compile(r"/commercial", re.I), 1.5),
    (re.compile(r"/insurance", re.I), 1.2),
    (re.compile(r"/services|/industries|/markets|/sectors", re.I), 1.1),
]

def score_page(text: str, url: str, phrase_map: Dict[str, float]) -> Tuple[float, List[str]]:
    """Return (score, evidence_lines)."""
    if not text: return 0.0, []
    score = 0.0
    ev = []
    for pat, wt in phrase_map.items():
        rx = re.compile(pat, re.I)
        hits = rx.findall(text)
        if hits:
            s = wt * len(hits)
            score += s
            ev.append(f"{len(hits)}× '{rx.pattern}' on {url} (+{s:.1f})")
    # URL boosts
    for rx, boost in URL_BOOSTS:
        if rx.search(url):
            score *= boost
            ev.append(f"url-boost {boost}× for {url}")
    return score, ev

def classify_with_evidence(pages: List[Tuple[str, str]]) -> Tuple[Set[str], Dict[str, List[str]]]:
    """
    pages: list of (url, text)
    Returns: (labels, evidence_per_label)
    """
    resi_total = comm_total = ins_total = 0.0
    resi_ev: List[str] = []; comm_ev: List[str] = []; ins_ev: List[str] = []
    saw_residential_word = False

    for url, txt in pages:
        if not txt: continue
        # quick explicit flag for residential
        if re.search(r"\bresidential\b", txt, re.I) or re.search(r"\bhomeowner(s)?\b", txt, re.I):
            saw_residential_word = True

        s, ev = score_page(txt, url, RESI_PHRASES)
        resi_total += s; resi_ev.extend(ev)

        s, ev = score_page(txt, url, COMM_PHRASES)
        comm_total += s; comm_ev.extend(ev)

        s, ev = score_page(txt, url, INS_PHRASES)
        ins_total += s; ins_ev.extend(ev)

    labels: Set[str] = set()
    evidence: Dict[str, List[str]] = {}

    # Decision rules:
    # 1) If we ever saw 'residential' or 'homeowner', include Residential.
    if saw_residential_word:
        labels.add("Residential")
        evidence.setdefault("Residential", []).append("Explicit term 'residential'/'homeowner' found (auto-include).")

    # 2) Add any vertical with a moderate score.
    if comm_total >= 1.5:
        labels.add("Commercial")
    if ins_total >= 1.2:
        labels.add("Insurance Driven")
    if resi_total >= 1.2:   # backstop if explicit wasn't seen but we have multiple hints
        labels.add("Residential")

    # 3) Soft fallback: if nothing yet, add whichever has the highest score >= 0.8
    if not labels:
        best = max(("Residential", resi_total), ("Commercial", comm_total), ("Insurance Driven", ins_total), key=lambda x: x[1])
        if best[1] >= 0.8:
            labels.add(best[0])

    # Save evidence snippets (top 5 per label unless DEBUG_EVIDENCE)
    def top(ev: List[str]) -> List[str]:
        if DEBUG_EVIDENCE:
            return ev[:12]
        return ev[:5]

    if "Residential" in labels:
        evidence["Residential"] = top(resi_ev) or evidence.get("Residential", [])
    if "Commercial" in labels:
        evidence["Commercial"] = top(comm_ev)
    if "Insurance Driven" in labels:
        evidence["Insurance Driven"] = top(ins_ev)

    return labels, evidence

# ---------------- Main ----------------
def main():
    if not API_TOKEN or not BOARD_ID:
        raise SystemExit("Set MONDAY_API_TOKEN and MONDAY_BOARD_ID in .env")

    board_name, columns = fetch_board_metadata(BOARD_ID)
    print(f"[INFO] Board: {board_name}")

    website_col = resolve_column(columns, WEBSITE_COL_TITLE)
    target_col  = resolve_column(columns, TARGET_DROPDOWN_COL_TITLE)

    if not website_col:
        raise SystemExit(f"Website column titled '{WEBSITE_COL_TITLE}' not found.")
    if not target_col:
        raise SystemExit(f"Dropdown column titled '{TARGET_DROPDOWN_COL_TITLE}' not found.")
    if target_col["type"] != "dropdown":
        raise SystemExit(f"Column '{TARGET_DROPDOWN_COL_TITLE}' must be a Dropdown (is {target_col['type']}).")

    website_col_id = website_col["id"]
    website_col_type = website_col["type"]
    target_col_id = target_col["id"]

    valid_labels = set(dropdown_label_names(columns, TARGET_DROPDOWN_COL_TITLE))
    needed = {"Residential","Commercial","Insurance Driven"}
    if not needed.issubset(valid_labels):
        raise SystemExit(f"Dropdown '{TARGET_DROPDOWN_COL_TITLE}' missing labels. Needs: {sorted(needed)} | Has: {sorted(valid_labels)}")

    # Optional: Name col for nicer logs
    name_col = resolve_column(columns, ITEM_NAME_COL_TITLE)
    name_col_id = name_col["id"] if name_col else None

    # Fetch items
    items = fetch_items(BOARD_ID)
    print(f"[INFO] Items: {len(items)}")

    for idx, it in enumerate(items, 1):
        col_map = {cv["id"]: (cv.get("text") or "") for cv in it.get("column_values", [])}
        display_name = col_map.get(name_col_id, it["name"]) if name_col_id else it["name"]

        # Extract website
        raw_site = extract_website_from_item(it, website_col_id, website_col_type)
        base = sanitize_url_for_home(raw_site)
        if not base:
            print(f"[{idx}] {display_name}: no valid website → skip (raw: {raw_site!r})")
            continue

        # Build crawl list
        seeds = [urljoin(base, p) for p in DEFAULT_PATHS]
        html_home = fetch_html(base)
        extra_links = discover_extra_links(base, html_home) if html_home else []
        urls = []
        seen = set()
        for u in seeds + extra_links:
            if u not in seen:
                urls.append(u); seen.add(u)

        # Collect (url, text)
        pages: List[Tuple[str,str]] = []
        if html_home:
            soup = BeautifulSoup(html_home, "html.parser")
            for tag in soup(["script","style","noscript"]): tag.decompose()
            home_text = " ".join(t.get_text(" ", strip=True) for t in soup.find_all(["h1","h2","h3","p","li","a","span","strong","em"]))
            pages.append((base, home_text))
        else:
            pages.append((base, page_text(base)))

        for u in urls[:1 + MAX_INTERNAL_LINKS_PER_SITE]:
            if u == base: 
                continue
            t = page_text(u)
            if t:
                pages.append((u, t))
            time.sleep(SLEEP_BETWEEN_FETCHES)

        labels, evidence = classify_with_evidence(pages)
        labels = labels.intersection(valid_labels)

        if labels:
            try:
                update_dropdown_labels(BOARD_ID, it["id"], target_col_id, sorted(labels))
                # concise reason print
                reasons = []
                for lab in sorted(labels):
                    ev_lines = evidence.get(lab, [])
                    if ev_lines:
                        reasons.append(f"{lab}: " + ev_lines[0])
                if not reasons and labels:
                    reasons = [f"{'/'.join(sorted(labels))} by keyword totals"]
                print(f"[{idx}] ✅ {display_name}: {base} → {sorted(labels)} | {' | '.join(reasons)}")
                if DEBUG_EVIDENCE:
                    for lab in sorted(labels):
                        for line in evidence.get(lab, [])[1:]:
                            print(f"      · {lab} evidence → {line}")
            except Exception as e:
                print(f"[{idx}] ❌ {display_name}: update failed → {e}")
        else:
            print(f"[{idx}] ⚠️ {display_name}: {base} → no clear labels")
        time.sleep(SLEEP_BETWEEN_COMPANIES)

    print("Done.")

if __name__ == "__main__":
    main()