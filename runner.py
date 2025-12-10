#!/usr/bin/env python3
"""
runner.py

Master script to run all your Monday.com enrichment scripts in sequence.

Usage:
    python runner.py          # ru
     yyyyy n all enrichment scripts once
    (or imported and called from webhook_server.py)

Assumptions:
  - All child scripts live in the same directory as this file.
  - Each child script reads MONDAY_API_TOKEN and MONDAY_BOARD_ID from .env.
  - Each child script is idempotent (skips rows where its column is already filled).
"""

import os
import sys
import subprocess
from datetime import datetime

# ====== EDIT THIS LIST TO MATCH YOUR ACTUAL SCRIPTS ======
SCRIPTS_TO_RUN = [
    "guarantee.py",           # fills Service Guarantees (text)
    "followers_cnt.py",     # fills Followers Count (numbers)
    "org_keywords.py",    # fills Organic/Top Organic Keywords (text)
    "ad_samples.py",
    "google_ads", 
    "fin_opt",
    "guarantee.py",
    "ins_vendor.py",
    "new_reviews.py",
    "bbb_check.py",
    "classify_target_verticals.py",
    "ig_active.py",
    "linkedin_active.py",
    "meta_ads.py",
    "skip_TV.py",
    "sponsors.py",
    "tiktok_active.py"
    # add more as you build them…
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(script_name: str, target_item_id: str | None) -> int:
    env = os.environ.copy()
    if target_item_id:
        env["TARGET_ITEM_ID"] = target_item_id
    return subprocess.run(
        [sys.executable, script_name],
        cwd=BASE_DIR,
        env=env,
        check=False,
    ).returncode

def run_all_scripts(target_item_id: str | None = None) -> int:
    for script in SCRIPTS_TO_RUN:
        run_script(script, target_item_id)


def main():
    code = run_all_scripts()
    sys.exit(code)


if __name__ == "__main__":
    main()