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


def run_script(script_name: str) -> int:
    """
    Run a single child script with the same Python interpreter.
    Return its exit code (0 = success, nonzero = failure).
    """
    script_path = os.path.join(BASE_DIR, script_name)
    if not os.path.isfile(script_path):
        print(f"[RUNNER] ERROR: Script not found: {script_path}")
        return 1

    print(f"\n[RUNNER] ==== Starting {script_name} at {datetime.now().isoformat(timespec='seconds')} ====")
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            check=False
        )
        print(f"[RUNNER] ==== Finished {script_name} with exit code {result.returncode} ====")
        return result.returncode
    except Exception as e:
        print(f"[RUNNER] EXCEPTION running {script_name}: {e}")
        return 1


def run_all_scripts() -> int:
    """
    Run all scripts in SCRIPTS_TO_RUN in order.
    Returns the worst (highest) exit code.
    """
    print(f"[RUNNER] Starting full enrichment run at {datetime.now().isoformat(timespec='seconds')}")
    worst_code = 0
    for script in SCRIPTS_TO_RUN:
        code = run_script(script)
        if code != 0:
            worst_code = max(worst_code, code)
    print(f"[RUNNER] Full run finished at {datetime.now().isoformat(timespec='seconds')} with overall status {worst_code}")
    return worst_code


def main():
    code = run_all_scripts()
    sys.exit(code)


if __name__ == "__main__":
    main()