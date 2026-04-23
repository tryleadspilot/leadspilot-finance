"""
LeadsPilot Wise Bot - API ONLY, no CSV
Uses two confirmed-working endpoints:
1. /v1/transfers - all bank transfers (confirmed: 155 results)
2. /v1/profiles/{pid}/balance-statements - card transactions only (CARD_TRANSACTION entries)
No double counting. No CSV. Bot reads everything itself.
"""
import os, re, json, logging, requests, threading, time, ssl, urllib.parse
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
    if not db: return
    db.cursor().execute("""
        DROP TABLE IF EXISTS tx;
        DROP TABLE IF EXISTS recipients;
        CREATE TABLE tx (
            id          TEXT PRIMARY KEY,
            created_at  TIMESTAMP,
            amount      NUMERIC(14,4),
            currency    TEXT,
            amount_aud  NUMERIC(14,4),
            name        TEXT,
            clean_name  TEXT,
            category    TEXT,
            tx_type     TEXT,
            status      TEXT DEFAULT 'completed',
            is_new      BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE recipients (
            account_id  TEXT PRIMARY KEY,
            name        TEXT,
            clean_name  TEXT,
            category    TEXT,
            note        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tx_date ON tx(created_at);
        CREATE INDEX IF NOT EXISTS idx_tx_cat  ON tx(category);
        CREATE INDEX IF NOT EXISTS idx_tx_new  ON tx(is_new);
    """)
    log.info("DB ready")

# ── RECIPIENT STORE ───────────────────────────────────────────────────────────
def get_recipient(account_id):
    db = get_db()
    if not db or not account_id: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT name,clean_name,category,note FROM recipients WHERE account_id=%s", (str(account_id),))
        return cur.fetchone()
    except: return None

def save_recipient(account_id, name, clean_name, category, note=""):
    db = get_db()
    if not db or not account_id: return
    try:
        db.cursor().execute(
            "INSERT INTO recipients(account_id,name,clean_name,category,note) VALUES(%s,%s,%s,%s,%s) "
            "ON CONFLICT(account_id) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, note=EXCLUDED.note",
            (str(account_id), name, clean_name, category, note))
        db.cursor().execute(
            "UPDATE tx SET clean_name=%s, category=%s WHERE name=%s AND clean_name IS NULL",
            (clean_name, category, name))
    except Exception as e: log.error(f"save_recipient: {e}")

def save_tx(tx_id, created_at, amount, currency, amount_aud, name, clean_name, category, tx_type, status="completed", is_new=False):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO tx(id,created_at,amount,currency,amount_aud,name,clean_name,category,tx_type,status,is_new) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, status=EXCLUDED.status",
            (str(tx_id), created_at, float(amount), currency, float(amount_aud),
             name, clean_name, category, tx_type, status, is_new))
    except Exception as e: log.error(f"save_tx {tx_id}: {e}")

def tx_exists(tx_id):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM tx WHERE id=%s", (str(tx_id),))
        return cur.fetchone() is not None
    except: return False

# ── CURRENCY ──────────────────────────────────────────────────────────────────
_fx = {}
def to_aud(amount, currency):
    if currency == "AUD": return float(amount)
    if currency not in _fx:
        try:
            r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{currency}", timeout=5)
            _fx[currency] = r.json()["rates"]["AUD"]
        except:
            _fx[currency] = {"USD":1.56,"GBP":1.97,"EUR":1.72,"PKR":0.0056,"PHP":0.027}.get(currency, 1.0)
    return round(float(amount) * _fx[currency], 4)

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
def resolve_account_name(account_id):
    """Get real recipient name from Wise account ID. Cached."""
    if not account_id: return None
    key = str(account_id)
    if key in _name_cache: return _name_cache[key]
    rec = get_recipient(key)
    if rec and rec[0]: _name_cache[key] = rec[0]; return rec[0]
    try:
        r = requests.get(f"{WISE_BASE}/v1/accounts/{account_id}",
            headers=wise_h(), timeout=6)
        if r.status_code == 200:
            d = r.json()
            name = (d.get("accountHolderName") or d.get("name") or
                    (d.get("details") or {}).get("accountHolderName"))
            if name:
                _name_cache[key] = name
                return name
    except: pass
    return None

def claude_categorize(name, amount_aud, currency, note=""):
    """Use Claude to categorize a transaction intelligently."""
    system = """You are an accountant for LeadsPilot, an SMS lead gen business based in Australia.
Given a transaction, return JSON only:
{"clean_name": "readable name", "category": "category"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Hardware, Business Other, Loan/Personal
Examples:
- Signal House, Sendivo → SMS Cost
- Fanbasis, Zeeshan Shabbir, Ghulam Shabir → Data Provider  
- Filipino names (Starla, Nina, Queenzen, John, Annalyn) → Salary
- Anthropic, Claude, Zoom, Slack, HighLevel, Google, N8n, Framer, Retell, Grasshopper, Opus Virtual, Bizee, Calendly, Instantly, Whop → Software
- Muhammad Hisham, Abdul Rehman → Personal
- Interactive Brokers → Investment
- Usman Ahmed → Rent
- MOEEZ MAZHAR, Abdul Rehman Tahir, Pak Mac → Hardware
- Wahaj Khan, shayan amir khan → Loan/Personal
Return JSON only, no markdown."""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":100,"system":system,
                  "messages":[{"role":"user","content":f"Name: {name}\nAmount: {amount_aud:.2f} AUD\nCurrency: {currency}\n{note}"}]},
            timeout=10)
        if r.status_code == 200:
            text = r.json()["content"][0]["text"].strip()
            return json.loads(text)
    except: pass
    return {"clean_name": name, "category": "Unknown"}

# ── SYNC ──────────────────────────────────────────────────────────────────────
def sync_transfers(is_new_run=False):
    """Load all bank transfers via /v1/transfers."""
    pid = get_pid(); offset = 0; new_txs = []
    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(),
                params={"profile":pid,"limit":100,"offset":offset}, timeout=20)
            r.raise_for_status()
            batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
            if not batch: break
            for tx in batch:
                status = tx.get("status","")
                if status not in ("outgoing_payment_sent","processing","incoming_payment_waiting"): continue
                tx_id  = str(tx.get("id",""))
                if not tx_id: continue
                is_new = is_new_run and not tx_exists(tx_id)
                amount = float(tx.get("sourceValue") or 0)
                cur    = tx.get("sourceCurrency","AUD")
                if amount == 0: continue
                aud = to_aud(amount, cur)
                # Get real recipient name
                acct_id = tx.get("targetAccount")
                name = resolve_account_name(acct_id) if acct_id else None
                if not name:
                    det = tx.get("details") or {}
                    name = det.get("reference") or f"transfer-{tx_id}"
                date_s = tx.get("created","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                except: d = datetime.now(timezone.utc)
                # Categorize with Claude
                rec = get_recipient(str(acct_id)) if acct_id else None
                if rec and rec[2]:
                    clean, cat = rec[1], rec[2]
                else:
                    result = claude_categorize(name, aud, cur)
                    clean = result.get("clean_name", name)
                    cat   = result.get("category", "Unknown")
                    if acct_id and cat != "Unknown":
                        save_recipient(str(acct_id), name, clean, cat)
                tx_status = "pending" if status in ("processing","incoming_payment_waiting") else "completed"
                save_tx(tx_id, d, amount, cur, aud, name, clean, cat, "TRANSFER", tx_status, is_new)
                if is_new:
                    new_txs.append((tx_id, name, clean, aud, cur, amount, d, tx_status))
            if len(batch) < 100: break
            offset += 100
        except Exception as e: log.error(f"sync_transfers: {e}"); break
    log.info(f"Transfers: loaded. New: {len(new_txs)}")
    return new_txs

def sync_card_transactions():
    """
    Load card transactions from balance statements.
    ONLY entries with referenceNumber starting with CARD_TRANSACTION.
    These are card payments — completely separate from bank transfers.
    """
    pid = get_pid(); total = 0
    now   = datetime.now(timezone.utc)
    start = datetime(2025, 11, 1, tzinfo=timezone.utc)
    try:
        bal_r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
            headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
        bal_r.raise_for_status()
        balances = [(b["id"], b["amount"]["currency"]) for b in bal_r.json() if b.get("id")]
    except: return 0

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
                    # ONLY card transactions — identified by referenceNumber
                    ref = str(tx.get("referenceNumber") or "")
                    if not ref.startswith("CARD_TRANSACTION"): continue
                    if tx_exists(ref): continue
                    val = float(tx.get("amount",{}).get("value",0))
                    if val >= 0: continue  # only debits
                    amount = abs(val)
                    aud = to_aud(amount, currency)
                    det = tx.get("details",{}) or {}
                    # Get merchant name
                    name = ""
                    for key in ["merchant","senderName","description"]:
                        v = det.get(key)
                        if v and isinstance(v, str) and v.strip():
                            name = v.strip(); break
                    if not name: continue
                    if any(s in name.lower() for s in ("divisible","leads pilot")): continue
                    date_s = tx.get("date") or tx.get("createdAt","")
                    try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                    except: d = datetime.now(timezone.utc)
                    # Categorize with Claude
                    result = claude_categorize(name, aud, currency)
                    clean = result.get("clean_name", name)
                    cat   = result.get("category", "Unknown")
                    save_tx(ref, d, amount, currency, aud, name, clean, cat, "CARD")
                    total += 1
            except Exception as e: log.error(f"card chunk: {e}")
            chunk_end = chunk_start
    log.info(f"Card transactions: {total}")
    return total

# ── QUERIES ───────────────────────────────────────────────────────────────────
def db_stats():
    db = get_db()
    if not db: return 0, None, None
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM tx")
        return cur.fetchone()
    except: return 0, None, None

def db_summary(start=None, end=None, limit=30):
    db = get_db()
    if not db: return [], []
    try:
        cur = db.cursor()
        # By category
        q = "SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*) FROM tx WHERE 1=1"
        p = []
        if start: q += " AND created_at>=%s"; p.append(start)
        if end:   q += " AND created_at<=%s"; p.append(end)
        q += " GROUP BY COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC"
        cur.execute(q, p); cats = cur.fetchall()
        # By recipient
        q2 = ("SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*), COALESCE(category,'Unknown') "
              "FROM tx WHERE 1=1")
        p2 = []
        if start: q2 += " AND created_at>=%s"; p2.append(start)
        if end:   q2 += " AND created_at<=%s"; p2.append(end)
        q2 += f" GROUP BY COALESCE(clean_name,name),COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC LIMIT {limit}"
        cur.execute(q2, p2); recips = cur.fetchall()
        return cats, recips
    except: return [], []

def db_recent(n=40):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT created_at, amount_aud, currency, amount,
                   COALESCE(clean_name,name), COALESCE(category,'Unknown'), status
            FROM tx ORDER BY created_at DESC LIMIT %s
        """, (n,))
        return cur.fetchall()
    except: return []

# ── NOTIFY NEW TRANSACTIONS ───────────────────────────────────────────────────
_pending = {}  # tx_id -> info

def notify_new(tx_id, name, clean, aud, cur, orig, d, status, client):
    est = d.astimezone(EST).strftime("%b %d %I:%M %p EST") if d.tzinfo else str(d)
    pending_tag = " *(PENDING)*" if status == "pending" else ""
    verb = "is sending" if status == "pending" else "sent"

    if clean and clean != name:
        # Known recipient — just confirm
        msg = (f":white_check_mark: *New transaction{pending_tag}*\n"
               f"{est} — {aud:,.2f} AUD")
        if cur != "AUD": msg += f" ({orig:,.2f} {cur})"
        msg += f" to *{clean}*"
    else:
        # Unknown — ask
        msg = (f":question: *New transaction{pending_tag} — {est}*\n"
               f"You {verb} *{aud:,.2f} AUD*")
        if cur != "AUD": msg += f" ({orig:,.2f} {cur})"
        msg += f" to *`{name}`*\n\nWho is this? Just reply naturally."
        _pending[name] = {"tx_id": tx_id, "name": name, "aud": aud, "cur": cur, "orig": orig, "d": d}

    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"notify: {e}")

# ── ANSWER ────────────────────────────────────────────────────────────────────
def answer(q):
    try: bals = get_balances()
    except: bals = []
    count, oldest, newest = db_stats()
    now   = datetime.now(EST)
    today = now.date()
    month_start = today.replace(day=1)
    week_start  = today - timedelta(days=today.weekday())

    cats_all,  recips_all  = db_summary()
    cats_month,recips_month = db_summary(
        start=datetime(today.year, today.month, 1, tzinfo=timezone.utc),
        end=datetime.now(timezone.utc))
    recent = db_recent(40)

    bal_text    = "\n".join(f"  {b}" for b in bals) or "  unavailable"
    cat_all     = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in cats_all) or "  none"
    cat_month   = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in cats_month) or "  none"
    rec_all     = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips_all) or "  none"
    rec_month   = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips_month) or "  none"

    def fmt(r):
        t = r[0].astimezone(EST).strftime("%b %d %I:%M %p EST") if r[0] and r[0].tzinfo else str(r[0])
        a = f"{float(r[1]):,.2f} AUD"
        if r[2] != "AUD": a += f" ({float(r[3]):,.2f} {r[2]})"
        tag = " [PENDING]" if r[6] == "pending" else ""
        return f"  {t}: {a} -> {r[4]} [{r[5]}]{tag}"
    recent_text = "\n".join(fmt(r) for r in recent) or "  none"

    system = f"""You are the Wise accountant for LeadsPilot — Suleman's SMS lead gen business (AUD account).
Answer directly. All amounts in AUD. All times in EST. Today: {now.strftime('%b %d %Y %I:%M %p EST')}

BALANCES: {bal_text}
DB: {count} transactions ({oldest} to {newest})

THIS MONTH ({month_start}):
{cat_month}

THIS MONTH BY RECIPIENT:
{rec_month}

ALL TIME BY CATEGORY:
{cat_all}

ALL TIME BY RECIPIENT:
{rec_all}

RECENT TRANSACTIONS:
{recent_text}"""

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":600,"system":system,
                  "messages":[{"role":"user","content":q}]},timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"answer: {e}")
    return "Could not answer."

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Loading all Wise transactions from API..._")
        # Register webhook
        try:
            pid = get_pid()
            h   = {**wise_h(),"Content-Type":"application/json"}
            r   = requests.get(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",headers=h,timeout=10)
            existing = [s.get("trigger_on") for s in (r.json() if r.status_code==200 else [])]
            if "transfers#state-change" not in existing:
                url = f"{RAILWAY_URL}/webhook/wise"
                r2  = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
                    headers=h,timeout=10,
                    json={"name":"LeadsPilot","trigger_on":"transfers#state-change",
                          "delivery":{"version":"2.0.0","url":url}})
                log.info(f"Webhook: HTTP {r2.status_code}")
        except Exception as e: log.error(f"webhook reg: {e}")

        # Load all history
        sync_transfers(is_new_run=False)
        sync_card_transactions()

        count, oldest, newest = db_stats()
        bals = get_balances()
        cats, _ = db_summary()

        msg  = f"*Wise Accountant Bot online* :white_check_mark:\n"
        msg += f"*{count} transactions loaded* ({oldest.strftime('%b %d %Y') if oldest else '?'} to {newest.strftime('%b %d %Y') if newest else '?'})\n"
        msg += "*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if cats:
            msg += "\n\n*All-time spending by category:*\n"
            for cat, total, cnt in cats:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        msg += "\nFrom now on I'll notify you about every new transaction. Ask me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(180)
        try:
            new_txs = sync_transfers(is_new_run=True)
            for tx in new_txs:
                tx_id, name, clean, aud, cur, orig, d, status = tx
                notify_new(tx_id, name, clean, aud, cur, orig, d, status, client)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"LeadsPilot Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    count, oldest, newest = db_stats()
    return jsonify({"status":"ok","transactions":count})

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status":"ok"}), 200
    try:
        payload = request.get_json(force=True) or {}
        log.info(f"Webhook: {payload.get('event_type','')}")
        def handle():
            new_txs = sync_transfers(is_new_run=True)
            for tx in new_txs:
                tx_id, name, clean, aud, cur, orig, d, status = tx
                notify_new(tx_id, name, clean, aud, cur, orig, d, status, slack_app.client)
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

    # Check if replying about a pending unknown transaction
    if _pending:
        # Use Claude to extract who the user is describing
        pending_names = list(_pending.keys())
        system = f"""The user is describing a transaction. Pending unknowns: {pending_names}
Extract: {{"name": "which pending name they're describing", "clean_name": "clean name", "category": "category", "note": "description"}}
If not about a transaction return: {{"not_tx": true}}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Hardware, Business Other, Loan/Personal
Return JSON only."""
        try:
            r = requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-haiku-4-5","max_tokens":150,"system":system,
                      "messages":[{"role":"user","content":text}]},timeout=10)
            if r.status_code == 200:
                data = json.loads(r.json()["content"][0]["text"].strip())
                if "not_tx" not in data and data.get("name") and data["name"] in _pending:
                    ctx = _pending.pop(data["name"])
                    clean = data.get("clean_name", ctx["name"])
                    cat   = data.get("category","Unknown")
                    note  = data.get("note","")
                    db = get_db()
                    if db:
                        db.cursor().execute(
                            "UPDATE tx SET clean_name=%s, category=%s, is_new=FALSE WHERE id=%s",
                            (clean, cat, ctx["tx_id"]))
                    say(f":white_check_mark: Got it! *{ctx['name']}* = *{clean}* ({cat}). Logged.")
                    return
        except: pass

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
    log.info("Starting Wise Bot...")
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    threading.Timer(5, startup, args=[slack_app.client]).start()
    threading.Thread(target=worker, args=[slack_app.client], daemon=True).start()
    handler.start()
