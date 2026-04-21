"""
Run this ONCE to register Wise webhooks via API.
Usage: WISE_API_KEY=your_key WEBHOOK_URL=https://your-railway-url.up.railway.app python register_wise_webhook.py
"""
import os, requests, json

WISE_KEY    = os.environ["WISE_API_KEY"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
WISE_BASE   = "https://api.wise.com"

headers = {"Authorization": f"Bearer {WISE_KEY}", "Content-Type": "application/json"}

# Get profile ID
r = requests.get(f"{WISE_BASE}/v1/profiles", headers=headers)
profiles = r.json()
pid = None
for p in profiles:
    if p.get("type") == "business":
        pid = p["id"]; break
if not pid:
    pid = profiles[0]["id"]
print(f"Profile ID: {pid}")

# Register balances#update webhook — fires on EVERY debit/credit (card + transfer)
payload = {
    "name": "LeadsPilot Balance Updates",
    "trigger_on": "balances#update",
    "delivery": {
        "version": "2.0.0",
        "url": f"{WEBHOOK_URL}/webhook/wise"
    }
}
r = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
    headers=headers, json=payload)
print(f"balances#update: {r.status_code} — {r.text[:200]}")

# Register transfers#state-change webhook — fires on every bank transfer
payload2 = {
    "name": "LeadsPilot Transfer State Changes",
    "trigger_on": "transfers#state-change",
    "delivery": {
        "version": "2.0.0",
        "url": f"{WEBHOOK_URL}/webhook/wise"
    }
}
r2 = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
    headers=headers, json=payload2)
print(f"transfers#state-change: {r2.status_code} — {r2.text[:200]}")

# List all subscriptions to confirm
r3 = requests.get(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions", headers=headers)
print(f"\nAll subscriptions: {json.dumps(r3.json(), indent=2)[:500]}")
