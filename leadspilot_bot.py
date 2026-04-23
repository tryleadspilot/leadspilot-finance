"""
LeadsPilot Wise Accountant Bot
- Silently loads ALL Wise history on startup
- Asks about NEW transactions only (going forward)
- Natural language replies — Claude categorizes automatically
- EST timezone throughout
- Self-registers Wise webhook on startup
"""
import os, re, json, logging, requests, threading, time, ssl, urllib.parse, hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
WISE_KEY        = os.environ["WISE_API_KEY"]
AI_KEY          = os.environ["ANTHROPIC_API_KEY"]
DATABASE_URL    = os.environ.get("DATABASE_URL", "")
CHANNEL_ID      = os.environ.get("CHANNEL_ID", "C0AUJHKE5C1")
PORT            = int(os.environ.get("PORT", 8080))
RAILWAY_URL     = os.environ.get("RAILWAY_URL", "https://leadspilot-finance-production.up.railway.app")
EST             = ZoneInfo("America/New_York")
WISE_BASE       = "https://api.wise.com"

# ── DB ────────────────────────────────────────────────────────────────────────
_db = None
def get_db():
    global _db
    if not DATABASE_URL: return None
    try:
        import pg8000.dbapi
        if _db is None:
            u = urllib.parse.urlparse(DATABASE_URL)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            _db = pg8000.dbapi.connect(
                host=u.hostname, port=u.port or 5432,
                database=u.path.lstrip("/"),
                user=u.username, password=u.password, ssl_context=ctx)
            _db.autocommit = True
        return _db
    except Exception as e:
        log.error(f"DB: {e}"); _db = None; return None

def init_db():
    db = get_db()
    if not db: log.warning("No DB"); return
    db.cursor().execute("""
        DROP TABLE IF EXISTS transactions;
        DROP TABLE IF EXISTS recipients;

        CREATE TABLE transactions (
            id           TEXT PRIMARY KEY,
            created_at   TIMESTAMP,
            amount       NUMERIC(14,4),
            currency     TEXT,
            amount_aud   NUMERIC(14,4),
            recipient_id TEXT,
            name         TEXT,
            clean_name   TEXT,
            category     TEXT,
            note         TEXT,
            tx_type      TEXT,
            status       TEXT DEFAULT 'completed',
            is_expense   BOOLEAN DEFAULT TRUE,
            is_new       BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE recipients (
            id         TEXT PRIMARY KEY,
            name       TEXT,
            clean_name TEXT,
            category   TEXT,
            note       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(created_at);
        CREATE INDEX IF NOT EXISTS idx_tx_cat  ON transactions(category);
        CREATE INDEX IF NOT EXISTS idx_tx_new  ON transactions(is_new);
    """)
    log.info("DB initialized fresh")

# ── HELPERS ───────────────────────────────────────────────────────────────────
def to_aud(amount, currency):
    if currency == "AUD": return float(amount)
    try:
        r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{currency}", timeout=5)
        rate = r.json()["rates"]["AUD"]
        return round(float(amount) * rate, 4)
    except:
        rates = {"USD":1.56,"GBP":1.97,"EUR":1.72,"PKR":0.0056,"PHP":0.027}
        return round(float(amount) * rates.get(currency, 1.0), 4)

def get_recipient(recipient_id):
    db = get_db()
    if not db or not recipient_id: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT name,clean_name,category,note FROM recipients WHERE id=%s", (str(recipient_id),))
        return cur.fetchone()
    except: return None

def save_recipient(recipient_id, name, clean_name, category, note=""):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO recipients(id,name,clean_name,category,note) VALUES(%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, note=EXCLUDED.note",
            (str(recipient_id), name, clean_name, category, note))
        db.cursor().execute(
            "UPDATE transactions SET clean_name=%s, category=%s, note=%s WHERE recipient_id=%s",
            (clean_name, category, note, str(recipient_id)))
    except Exception as e: log.error(f"save_recipient: {e}")

def tx_exists(tx_id):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM transactions WHERE id=%s", (str(tx_id),))
        return cur.fetchone() is not None
    except: return False

def save_tx(tx_id, created_at, amount, currency, amount_aud, recipient_id, name, clean_name, category, tx_type, is_new=False, status="completed"):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO transactions(id,created_at,amount,currency,amount_aud,recipient_id,name,clean_name,category,tx_type,status,is_new) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=EXCLUDED.clean_name, category=EXCLUDED.category, status=EXCLUDED.status",
            (str(tx_id), created_at, float(amount), currency, float(amount_aud),
             str(recipient_id) if recipient_id else None,
             name, clean_name, category, tx_type, status, is_new))
    except Exception as e: log.error(f"save_tx: {e}")


# Fast category lookup — no API calls needed for known names
_KNOWN_CATS = {
    "signal house": ("Signal House SMS", "SMS Cost"),
    "sendivo": ("Sendivo", "SMS Cost"),
    "fanbasis": ("Fanbasis (Jacob)", "Data Provider"),
    "zeeshan": ("Zeeshan Shabbir", "Data Provider"),
    "ghulam shabir": ("Zeeshan Shabbir", "Data Provider"),
    "muhammad hisham": ("Muhammad Hisham", "Personal"),
    "abdul rehman tahir": ("Abdul Rehman Tahir", "Hardware"),
    "abdul rehman": ("Abdul Rehman", "Personal"),
    "starla": ("Starla", "Salary"),
    "nina selvendy": ("Nina", "Salary"),
    "john isaac": ("John", "Salary"),
    "queenzen": ("Queenzen", "Salary"),
    "annalyn": ("Annalyn", "Salary"),
    "interactive brokers": ("Interactive Brokers", "Investment"),
    "usman ahmed": ("Usman Ahmed", "Rent"),
    "wahaj": ("Wahaj Khan", "Loan/Personal"),
    "shayan amir": ("Wahaj Khan", "Loan/Personal"),
    "moeez": ("Moeez Mazhar", "Hardware"),
    "pak mac": ("Pak Mac AC", "Hardware"),
    "anthropic": ("Anthropic", "Software"),
    "claude": ("Claude", "Software"),
    "highlevel": ("HighLevel", "Software"),
    "high level": ("HighLevel", "Software"),
    "calendly": ("Calendly", "Software"),
    "opus virtual": ("Opus Virtual Offices", "Software"),
    "zoom": ("Zoom", "Software"),
    "n8n": ("N8n Cloud", "Software"),
    "slack": ("Slack", "Software"),
    "framer": ("Framer", "Software"),
    "google": ("Google Workspace", "Software"),
    "retell": ("Retell AI", "Software"),
    "instantly": ("Instantly", "Software"),
    "whop": ("Whop", "Software"),
    "grasshopper": ("Grasshopper", "Software"),
    "bizee": ("Bizee", "Software"),
    "onlinejobs": ("OnlineJobs.ph", "Software"),
    "mulebuy": ("Mulebuy", "Personal"),
    "rinip": ("Whop Rinip Ventures", "Business Other"),
    "saurabh": ("Saurabh Kumar", "Business Other"),
    "abdullah habib": ("Abdullah Habib", "Business Other"),
    "inyxel": ("Inyxel Studios", "Business Other"),
}

def quick_categorize(name):
    """Fast lookup for known recipients — no API call needed."""
    if not name: return None, None
    low = name.lower()
    for key, (clean, cat) in _KNOWN_CATS.items():
        if key in low:
            return clean, cat
    return None, None

def get_pending_new():
    """Get new transactions that haven't been identified yet."""
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT id, created_at, amount_aud, currency, amount, name, recipient_id
            FROM transactions
            WHERE is_new=TRUE AND (clean_name IS NULL OR category IS NULL)
            ORDER BY created_at DESC
        """)
        return cur.fetchall()
    except: return []

def mark_identified(tx_id):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute("UPDATE transactions SET is_new=FALSE WHERE id=%s", (str(tx_id),))
    except: pass

# ── WISE API ──────────────────────────────────────────────────────────────────
def wise_h(): return {"Authorization": f"Bearer {WISE_KEY}"}

_pid = None
def get_pid():
    global _pid
    if _pid: return _pid
    r = requests.get(f"{WISE_BASE}/v1/profiles", headers=wise_h(), timeout=10)
    r.raise_for_status()
    for p in r.json():
        if p.get("type") == "business": _pid = str(p["id"]); return _pid
    _pid = str(r.json()[0]["id"]); return _pid

def get_balances():
    pid = get_pid()
    r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
        headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
    r.raise_for_status()
    out = []
    for b in r.json():
        v = float(b.get("amount",{}).get("value",0))
        c = b.get("amount",{}).get("currency","")
        if v > 0: out.append(f"{c}: {v:,.2f}")
    return out

_name_cache = {}
def get_account_name(account_id):
    """Get real name from Wise account ID."""
    if not account_id: return None
    key = str(account_id)
    if key in _name_cache: return _name_cache[key]
    # Check DB first
    r = get_recipient(key)
    if r and r[0]: _name_cache[key] = r[0]; return r[0]
    # Call API
    try:
        resp = requests.get(f"{WISE_BASE}/v1/accounts/{account_id}",
            headers=wise_h(), timeout=6)
        if resp.status_code == 200:
            d = resp.json()
            name = (d.get("accountHolderName") or d.get("name") or
                    (d.get("details") or {}).get("accountHolderName"))
            if name: _name_cache[key] = name; return name
    except: pass
    return None

def load_all_history():
    """
    Silently load ALL Wise transaction history into DB.
    Uses /v1/transfers for bank transfers.
    Uses balance statements for card transactions.
    NO questions asked — just loads data.
    """
    pid = get_pid()
    total = 0

    # ── Bank transfers ──
    offset = 0
    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(), params={"profile":pid,"limit":100,"offset":offset}, timeout=20)
            r.raise_for_status()
            batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
            if not batch: break
            for tx in batch:
                status = tx.get("status","")
                if status not in ("outgoing_payment_sent","processing","incoming_payment_waiting","funds_converted"): continue
                tx_id  = str(tx.get("id",""))
                amount = float(tx.get("sourceValue") or 0)
                cur    = tx.get("sourceCurrency","AUD")
                if amount == 0 or not tx_id: continue
                aud = to_aud(amount, cur)
                acct_id = tx.get("targetAccount")
                name = get_account_name(acct_id) if acct_id else None
                if not name:
                    det = tx.get("details") or {}
                    name = det.get("reference") or f"transfer-{tx_id}"
                date_s = tx.get("created","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                except: d = datetime.now(timezone.utc)
                # Check if already known recipient
                recip_data = get_recipient(str(acct_id)) if acct_id else None
                clean = recip_data[1] if recip_data else None
                cat   = recip_data[2] if recip_data else None
                tx_status = "pending" if tx.get("status") in ("processing","incoming_payment_waiting") else "completed"
                save_tx(tx_id, d, amount, cur, aud, acct_id, name, clean, cat, "TRANSFER", False, tx_status)
                total += 1
            if len(batch) < 100: break
            offset += 100
        except Exception as e: log.error(f"load transfers: {e}"); break

    # ── Card transactions via balance statements ──
    try:
        bal_r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
            headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
        bal_r.raise_for_status()
        balances = [(b["id"], b["amount"]["currency"]) for b in bal_r.json() if b.get("id")]
    except: balances = []

    now   = datetime.now(timezone.utc)
    start = datetime(2025, 11, 1, tzinfo=timezone.utc)

    for bal_id, currency in balances:
        chunk_end = now
        while chunk_end > start:
            chunk_start = max(chunk_end - timedelta(days=89), start)
            try:
                r = requests.get(
                    f"{WISE_BASE}/v1/profiles/{pid}/balance-statements/{bal_id}/statement.json",
                    headers=wise_h(),
                    params={"currency":currency,
                            "intervalStart":chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "intervalEnd":chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "type":"COMPACT"}, timeout=25)
                if r.status_code != 200: break
                for tx in r.json().get("transactions",[]):
                    # ONLY card transactions — bank transfers handled by /v1/transfers
                    ref = str(tx.get("referenceNumber") or "")
                    if not ref.startswith("CARD_TRANSACTION"): continue
                    val = float(tx.get("amount",{}).get("value",0))
                    if val >= 0: continue  # only debits
                    amount = abs(val)
                    det = tx.get("details",{}) or {}
                    name = ""
                    for key in ["merchant","senderName","description"]:
                        v = det.get(key)
                        if v and isinstance(v, str) and v.strip():
                            name = v.strip(); break
                    if not name: continue
                    if any(s in name.lower() for s in ("divisible","leads pilot")): continue
                    aud = to_aud(amount, currency)
                    date_s = tx.get("date") or tx.get("createdAt","")
                    try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                    except: d = datetime.now(timezone.utc)
                    clean, cat = quick_categorize(name)
                    save_tx(ref, d, amount, currency, aud, None, name, clean, cat, "CARD", False)
                    total += 1
            except Exception as e: log.error(f"stmt chunk: {e}")
            chunk_end = chunk_start

    log.info(f"History loaded: {total} transactions")
    return total

def get_new_transfers():
    """
    Check for new transfers since last sync.
    Returns list of NEW transactions not yet in DB.
    """
    pid = get_pid()
    new_txs = []
    try:
        r = requests.get(f"{WISE_BASE}/v1/transfers",
            headers=wise_h(), params={"profile":pid,"limit":20,"offset":0}, timeout=20)
        r.raise_for_status()
        batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
        for tx in batch:
            status = tx.get("status","")
            if status not in ("outgoing_payment_sent","processing","incoming_payment_waiting","funds_converted"): continue
            tx_id = str(tx.get("id",""))
            if not tx_id or tx_exists(tx_id): continue
            amount = float(tx.get("sourceValue") or 0)
            cur    = tx.get("sourceCurrency","AUD")
            if amount == 0: continue
            aud = to_aud(amount, cur)
            acct_id = tx.get("targetAccount")
            name = get_account_name(acct_id) if acct_id else None
            if not name:
                det = tx.get("details") or {}
                name = det.get("reference") or f"transfer-{tx_id}"
            date_s = tx.get("created","")
            try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
            except: d = datetime.now(timezone.utc)
            recip_data = get_recipient(str(acct_id)) if acct_id else None
            clean = recip_data[1] if recip_data else None
            cat   = recip_data[2] if recip_data else None
            tx_status = "pending" if tx.get("status") in ("processing","incoming_payment_waiting") else "completed"
            save_tx(tx_id, d, amount, cur, aud, acct_id, name, clean, cat, "TRANSFER", True, tx_status)
            new_txs.append((tx_id, name, aud, cur, amount, acct_id, d, tx_status))
    except Exception as e: log.error(f"get_new_transfers: {e}")
    return new_txs

# ── CLAUDE INTELLIGENCE ───────────────────────────────────────────────────────
def claude_categorize(name, note, context=""):
    """Use Claude to intelligently categorize a transaction."""
    system = """You are an accountant for LeadsPilot, an SMS lead gen business.
Given a recipient name and description, return JSON:
{"clean_name": "readable name", "category": "category", "note": "brief note"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Hardware, Business Other, Loan/Personal
Be smart: Signal House/Sendivo = SMS Cost, Fanbasis/Zeeshan = Data Provider,
Filipino names = Salary, Anthropic/Claude/Zoom/Slack = Software, etc.
Return JSON only, no markdown."""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":200,"system":system,
                  "messages":[{"role":"user","content":f"Recipient: {name}\nDescription: {note}\nContext: {context}"}]},
            timeout=15)
        if r.status_code == 200:
            return json.loads(r.json()["content"][0]["text"].strip())
    except: pass
    return {"clean_name": name, "category": "Unknown", "note": note}

def claude_extract_reply(text, tx_name, tx_id):
    """Extract what the user said about a transaction."""
    system = """Extract info from user's reply about a transaction. Return JSON:
{"clean_name": "readable name", "category": "category", "note": "what they said"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Hardware, Business Other, Loan/Personal
If the reply is just a confirmation like "yes" or "ok", return: {"confirmed": true}
Return JSON only."""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":200,"system":system,
                  "messages":[{"role":"user","content":f"Transaction to: {tx_name}\nUser reply: {text}"}]},
            timeout=15)
        if r.status_code == 200:
            return json.loads(r.json()["content"][0]["text"].strip())
    except: pass
    return None

# ── NOTIFY NEW TRANSACTION ────────────────────────────────────────────────────
_pending_context = {}  # tx_id -> tx info, for when user replies

def notify_new_tx(tx_id, name, amount_aud, currency, amount_orig, acct_id, created_at, client, tx_status="completed"):
    """Post to Slack asking about a new transaction."""
    # Try Claude auto-categorize first
    auto = claude_categorize(name, "", f"amount: {amount_aud:.2f} AUD")
    
    # If Claude is confident, just confirm
    if auto.get("category") != "Unknown":
        clean  = auto.get("clean_name", name)
        cat    = auto.get("category","Unknown")
        note   = auto.get("note","")
        save_tx(tx_id, created_at, amount_orig, currency, amount_aud,
                acct_id, name, clean, cat, "TRANSFER", False)
        if acct_id: save_recipient(str(acct_id), name, clean, cat, note)
        est_time = created_at.astimezone(EST).strftime("%b %d %I:%M %p EST")
        status_icon = ":hourglass_flowing_sand:" if tx_status == "pending" else ":white_check_mark:"
        status_label = " *(PENDING — not sent yet)*" if tx_status == "pending" else ""
        msg = (f"{status_icon} *New transaction logged{status_label}:*\n"
               f"*{est_time}* — {amount_aud:,.2f} AUD")
        if currency != "AUD":
            msg += f" ({amount_orig:,.2f} {currency})"
        msg += f" to *{clean}* [{cat}]"
        if note: msg += f"\n_{note}_"
    else:
        # Ask Suleman
        est_time = created_at.astimezone(EST).strftime("%b %d %I:%M %p EST")
        status_label = " *(PENDING — not sent yet)*" if tx_status == "pending" else ""
        msg = (f":question: *New transaction{status_label} — {est_time}*\n"
               f"You {'are sending' if tx_status == 'pending' else 'sent'} *{amount_aud:,.2f} AUD*")
        if currency != "AUD":
            msg += f" ({amount_orig:,.2f} {currency})"
        msg += f" to *`{name}`*\n\n"
        msg += f"Who is this? Just reply naturally."
        _pending_context[tx_id] = {
            "name": name, "amount_aud": amount_aud, "currency": currency,
            "amount_orig": amount_orig, "acct_id": acct_id, "created_at": created_at
        }

    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"notify: {e}")

# ── WEBHOOK REGISTRATION ──────────────────────────────────────────────────────
def register_webhook():
    try:
        pid = get_pid()
        url = f"{RAILWAY_URL}/webhook/wise"
        h   = {**wise_h(), "Content-Type":"application/json"}
        r   = requests.get(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions", headers=h, timeout=10)
        existing = [s.get("trigger_on") for s in (r.json() if r.status_code==200 else [])]
        if "transfers#state-change" not in existing:
            r2 = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
                headers=h, timeout=10,
                json={"name":"LeadsPilot","trigger_on":"transfers#state-change",
                      "delivery":{"version":"2.0.0","url":url}})
            log.info(f"Webhook registered: HTTP {r2.status_code}")
        else:
            log.info("Webhook already registered")
    except Exception as e: log.error(f"register_webhook: {e}")

# ── AI ANSWER ─────────────────────────────────────────────────────────────────
def answer(q):
    db = get_db()
    if not db: return "DB not connected."
    try:
        # Get balances
        bals = get_balances()
        bal_text = "\n".join(f"  {b}" for b in bals)

        now   = datetime.now(EST)
        today = now.date()
        month_start = today.replace(day=1)
        week_start  = today - timedelta(days=today.weekday())

        # Category totals
        cur = db.cursor()
        cur.execute("""
            SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*)
            FROM transactions
            WHERE is_expense=TRUE
            AND COALESCE(name,'') NOT ILIKE '%divisible%'
            AND COALESCE(name,'') NOT ILIKE '%leads pilot%'
            GROUP BY COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC
        """)
        cats = cur.fetchall()

        # Recipient totals
        cur.execute("""
            SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*), COALESCE(category,'Unknown')
            FROM transactions
            WHERE is_expense=TRUE
            AND COALESCE(name,'') NOT ILIKE '%divisible%'
            AND COALESCE(name,'') NOT ILIKE '%leads pilot%'
            GROUP BY COALESCE(clean_name,name), COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC LIMIT 30
        """)
        recips = cur.fetchall()

        # This month
        cur.execute("""
            SELECT COALESCE(category,'Unknown'), SUM(amount_aud)
            FROM transactions
            WHERE created_at >= %s
            AND is_expense=TRUE
            AND COALESCE(name,'') NOT ILIKE '%divisible%'
            AND COALESCE(name,'') NOT ILIKE '%leads pilot%'
            GROUP BY COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC
        """, (month_start,))
        month_cats = cur.fetchall()

        # Recent 40
        cur.execute("""
            SELECT created_at, amount_aud, currency, amount, COALESCE(clean_name,name),
                   COALESCE(category,'Unknown'), COALESCE(status,'completed')
            FROM transactions
            WHERE is_expense=TRUE
            AND COALESCE(name,'') NOT ILIKE '%divisible%'
            AND COALESCE(name,'') NOT ILIKE '%leads pilot%'
            ORDER BY created_at DESC LIMIT 40
        """)
        recent = cur.fetchall()

        cat_text    = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in cats)
        recip_text  = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips)
        month_text  = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD" for r in month_cats)
        def fmt_recent(r):
            time_str = r[0].astimezone(EST).strftime("%b %d %I:%M %p EST") if r[0] else "?"
            status_tag = " [PENDING]" if len(r) > 6 and r[6] == "pending" else ""
            amt = f"{float(r[1]):,.2f} AUD"
            if r[2] != "AUD": amt += f" ({float(r[3]):,.2f} {r[2]})"
            return f"  {time_str}: {amt} -> {r[4]} [{r[5]}]{status_tag}"
        recent_text = "\n".join(fmt_recent(r) for r in recent)

        system = f"""You are the accountant for LeadsPilot — Suleman's SMS lead gen business (AUD account).
Answer directly. All times in EST. All amounts in AUD (show original currency in brackets if different).

TODAY (EST): {now.strftime('%b %d %Y %I:%M %p EST')}
MONTH: {month_start} | WEEK: {week_start}

BALANCES:
{bal_text}

ALL-TIME BY CATEGORY:
{cat_text}

THIS MONTH BY CATEGORY:
{month_text}

ALL-TIME BY RECIPIENT:
{recip_text}

RECENT TRANSACTIONS:
{recent_text}"""

        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":500,"system":system,
                  "messages":[{"role":"user","content":q}]},timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"answer: {e}")
    return "Could not answer."

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Loading all Wise transaction history into database..._")
        register_webhook()
        n = load_all_history()
        bals = get_balances()
        db = get_db()
        cats = []
        if db:
            cur = db.cursor()
            cur.execute("""
                SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*)
                FROM transactions
                WHERE is_expense=TRUE
                AND COALESCE(name,'') NOT ILIKE '%divisible%'
                AND COALESCE(name,'') NOT ILIKE '%leads pilot%'
                GROUP BY COALESCE(category,'Unknown')
                ORDER BY SUM(amount_aud) DESC
            """)
            cats = cur.fetchall()
            cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM transactions")
            count, oldest, newest = cur.fetchone()

        msg  = f"*Wise Accountant Bot online* :white_check_mark:\n"
        msg += f"*{count} transactions loaded* "
        if oldest: msg += f"({oldest.strftime('%b %d %Y')} to {newest.strftime('%b %d %Y')})\n"
        msg += "*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if cats:
            msg += "\n\n*Spending by category (all time):*\n"
            for cat, total, cnt in cats:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        msg += "\nFrom now on I'll ask you about every new transaction. Ask me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

# ── BACKGROUND WORKER ─────────────────────────────────────────────────────────
def worker(client):
    """Poll for new transfers every 3 minutes."""
    while True:
        time.sleep(180)
        try:
            new_txs = get_new_transfers()
            for tx in new_txs:
                tx_id, name, aud, cur, orig, acct_id, d = tx[0], tx[1], tx[2], tx[3], tx[4], tx[5], tx[6]
                tx_status = tx[7] if len(tx) > 7 else "completed"
                notify_new_tx(tx_id, name, aud, cur, orig, acct_id, d, client, tx_status)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"LeadsPilot Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    db = get_db()
    count = 0
    if db:
        try:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM transactions")
            count = cur.fetchone()[0]
        except: pass
    return jsonify({"status":"ok","transactions":count})

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status":"ok"}), 200
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type","")
        log.info(f"Webhook: {event_type}")
        if "transfer" in event_type.lower():
            def handle():
                new_txs = get_new_transfers()
                for tx in new_txs:
                    tx_id, name, aud, cur, orig, acct_id, d = tx[0],tx[1],tx[2],tx[3],tx[4],tx[5],tx[6]
                    tx_status = tx[7] if len(tx) > 7 else "completed"
                    notify_new_tx(tx_id, name, aud, cur, orig, acct_id, d, slack_app.client, tx_status)
            threading.Thread(target=handle, daemon=True).start()
        return jsonify({"status":"ok"}), 200
    except Exception as e:
        log.error(f"webhook: {e}"); return jsonify({"status":"error"}), 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>","",text or "").strip()
    if not text: return

    # Check if replying about a pending transaction
    if _pending_context:
        for tx_id, ctx in list(_pending_context.items()):
            result = claude_extract_reply(text, ctx["name"], tx_id)
            if result and "confirmed" not in result and result.get("category"):
                clean = result.get("clean_name", ctx["name"])
                cat   = result.get("category","Unknown")
                note  = result.get("note","")
                save_tx(tx_id, ctx["created_at"], ctx["amount_orig"], ctx["currency"],
                        ctx["amount_aud"], ctx["acct_id"], ctx["name"], clean, cat, "TRANSFER", False)
                if ctx["acct_id"]:
                    save_recipient(str(ctx["acct_id"]), ctx["name"], clean, cat, note)
                del _pending_context[tx_id]
                say(f":white_check_mark: Got it! *{ctx['name']}* = *{clean}* ({cat}). Logged.")
                return

    # Otherwise answer as accountant
    say("_Checking..._")
    say(answer(text))

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text",""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
    t = (event.get("text") or "").strip()
    if t: process(t, say)

if __name__ == "__main__":
    log.info("Starting Wise Accountant Bot...")
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    threading.Timer(5, startup, args=[slack_app.client]).start()
    threading.Thread(target=worker, args=[slack_app.client], daemon=True).start()
    handler.start()
