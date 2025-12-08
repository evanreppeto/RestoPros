#!/usr/bin/env python3
"""
webhook_server.py

Small Flask server that:
  - exposes POST /monday-hook
  - verifies a simple secret
  - runs all enrichment scripts (via runner.run_all_scripts) in a background thread

Intended to be triggered by Monday.com webhooks, e.g.
  - "When an item is created"
  - "When column changes" (Name, Address, Website)

Requirements:
    pip install flask python-dotenv
"""

import os
import json
import threading
from datetime import datetime
from typing import Any, Dict

from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load .env from this directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# Secret used to validate webhook calls (set in .env and in Monday URL)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Import the runnerp
import runner  # runner.py must be in the same directory

app = Flask(__name__)


def run_enrichment_async(event_payload: Dict[str, Any]) -> None:
    """
    Background job that actually runs the scripts.
    event_payload is just logged for debugging; runner works on the full board.
    """
    print("\n[WEBHOOK] ==== Enrichment run triggered ====")
    print(f"[WEBHOOK] Time: {datetime.now().isoformat(timespec='seconds')}")
    print(f"[WEBHOOK] Payload: {json.dumps(event_payload, indent=2)}")

    try:
        code = runner.run_all_scripts(target_item_id=that_id)
        print(f"[WEBHOOK] Enrichment finished with exit code {code}")
    except Exception as e:
        print(f"[WEBHOOK] ERROR during enrichment: {e}")


@app.route("/monday-hook", methods=["POST"])
def monday_hook():
    # 1. Check secret (simple protection)
    secret_from_query = request.args.get("secret", "")
    if WEBHOOK_SECRET and secret_from_query != WEBHOOK_SECRET:
        return jsonify({"status": "forbidden", "reason": "bad secret"}), 403

    # 2. Try to read JSON payload
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    # 3. Handle Monday "challenge" verification
    #    Monday sends something like: {"challenge": "some-random-string", ...}
    challenge = payload.get("challenge")
    if challenge:
        # Echo the challenge back EXACTLY as Monday sent it
        # This is required for Monday to verify the webhook URL.
        return jsonify({"challenge": challenge})

    # 4. Normal event: run enrichment in the background
    t = threading.Thread(target=run_enrichment_async, args=(payload,))
    t.daemon = True
    t.start()

    return jsonify({"status": "ok", "message": "Enrichment started"}), 200


if __name__ == "__main__":
    # Run Flask dev server (for local testing or behind something like ngrok)
    # You probably want host="0.0.0.0" so it's reachable externally.
    app.run(host="0.0.0.0", port=5000, debug=True)