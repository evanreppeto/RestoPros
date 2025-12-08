#!/usr/bin/env python3
import os, json, time, requests
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID  = os.getenv("MONDAY_BOARD_ID")  # keep as string
API_URL   = "https://api.monday.com/v2"
HEADERS   = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

COLUMN_TITLE   = "Target Verticals"         # dropdown column title
TARGET_LABELS  = ["Residential"]            # choose by NAME, e.g. ["Residential","Commercial"]
TARGET_LABEL_IDS = []                       # or choose by ID, e.g. [1,2]  (leave empty if using names)

def gql(query, variables=None):
    r = requests.post(API_URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data: raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def main():
    if not API_TOKEN or not BOARD_ID:
        raise SystemExit("Set MONDAY_API_TOKEN and MONDAY_BOARD_ID in .env")

    # 1) Column + labels
    meta_q = """
    query($ids:[ID!]){
      boards(ids:$ids){
        name
        columns{ id title type settings_str }
      }
    }"""
    d = gql(meta_q, {"ids":[BOARD_ID]})
    board = d["boards"][0]
    cols  = board["columns"]

    col = next((c for c in cols if c["title"] == COLUMN_TITLE), None)
    if not col:
        raise SystemExit(f"Column '{COLUMN_TITLE}' not found on '{board['name']}'")
    if col["type"] != "dropdown":
        raise SystemExit(f"Column '{COLUMN_TITLE}' is type '{col['type']}', expected 'dropdown'.")

    col_id = col["id"]
    settings = {}
    if col.get("settings_str"):
        try: settings = json.loads(col["settings_str"])
        except: pass
    raw_labels = settings.get("labels", [])
    # Normalize to simple structures:
    label_names = [x["name"] if isinstance(x, dict) else str(x) for x in raw_labels]
    label_ids   = [x["id"]   if isinstance(x, dict) else None    for x in raw_labels]
    name_to_id  = {n: i for n, i in zip(label_names, label_ids)}
    id_to_name  = {i: n for n, i in zip(label_names, label_ids)}

    print(f"[INFO] Board: {board['name']}")
    print(f"[INFO] Dropdown '{COLUMN_TITLE}' id={col_id}")
    print(f"[INFO] Labels (id → name): {', '.join([f'{i}:{n}' for i,n in id_to_name.items()])}")

    # 2) Build the payload using names or ids (prefer names if provided)
    payload = {}
    if TARGET_LABELS:
        # Validate names
        missing = [n for n in TARGET_LABELS if n not in name_to_id]
        if missing:
            raise SystemExit(f"Unknown label name(s): {missing}. Valid: {label_names}")
        payload[col_id] = {"labels": TARGET_LABELS}
    elif TARGET_LABEL_IDS:
        # Validate ids
        missing_ids = [i for i in TARGET_LABEL_IDS if i not in id_to_name]
        if missing_ids:
            raise SystemExit(f"Unknown label id(s): {missing_ids}. Valid ids: {label_ids}")
        payload[col_id] = {"labels_ids": TARGET_LABEL_IDS}
    else:
        raise SystemExit("Set either TARGET_LABELS (names) or TARGET_LABEL_IDS (ids).")

    # 3) Fetch ALL items
    items, cursor = [], None
    q_items = """
    query($board_id:[ID!], $cursor:String){
      boards(ids:$board_id){
        items_page(limit:500, cursor:$cursor){
          items{ id name }
          cursor
        }
      }
    }"""
    while True:
        data = gql(q_items, {"board_id":[BOARD_ID], "cursor":cursor})
        page = data["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor: break
    print(f"[INFO] Updating {len(items)} items...")

    # 4) Update each item
    mutation = """
    mutation($board_id:ID!, $item_id:ID!, $vals: JSON!){
      change_multiple_column_values(board_id:$board_id, item_id:$item_id, column_values:$vals){ id }
    }"""
    vals = json.dumps(payload)

    for i, it in enumerate(items, 1):
        try:
            gql(mutation, {"board_id": BOARD_ID, "item_id": it["id"], "vals": vals})
            pretty = TARGET_LABELS if TARGET_LABELS else [id_to_name[i] for i in TARGET_LABEL_IDS]
            print(f"[{i}/{len(items)}] ✅ {it['name']} ({it['id']}) -> {pretty}")
        except Exception as e:
            print(f"[{i}/{len(items)}] ❌ {it['name']} ({it['id']}): {e}")
        time.sleep(0.2)

    print("✅ Done.")

if __name__ == "__main__":
    main()