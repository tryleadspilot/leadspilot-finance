"""
Wise Accountant Bot - Clean Build
==================================
1. On startup: download ALL Wise history silently into DB (no questions)
2. New transactions: ask Suleman who it is, Claude categorizes reply
3. EST timezone throughout
4. Webhook for instant bank transfer notifications
5. Polls every 3 min for card transactions (Wise API limitation)
"""
import os, re, json, logging, requests, threading, time, ssl, urllib.parse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
        DROP TABLE IF EXISTS wise_tx;
        DROP TABLE IF EXISTS known_recipients;

        CREATE TABLE wise_tx (
            id          TEXT PRIMARY KEY,
            created_at  TIMESTAMP WITH TIME ZONE,
            amount      NUMERIC(14,4),
            currency    TEXT,
            amount_aud  NUMERIC(14,4),
            name        TEXT,
            clean_name  TEXT,
            category    TEXT,
            note        TEXT,
            tx_type     TEXT,
            is_new      BOOLEAN DEFAULT FALSE,
            synced_at   TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE known_recipients (
            name        TEXT PRIMARY KEY,
            clean_name  TEXT,
            category    TEXT,
            note        TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_created ON wise_tx(created_at);
        CREATE INDEX IF NOT EXISTS idx_new ON wise_tx(is_new);
        CREATE INDEX IF NOT EXISTS idx_cat ON wise_tx(category);
    """)
    log.info("DB ready")

def save_tx(tx_id, created_at, amount, currency, amount_aud, name, clean_name, category, note, tx_type, is_new=False):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO wise_tx(id,created_at,amount,currency,amount_aud,name,clean_name,category,note,tx_type,is_new) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=COALESCE(EXCLUDED.clean_name,wise_tx.clean_name), "
            "category=COALESCE(EXCLUDED.category,wise_tx.category)",
            (tx_id, created_at, float(amount), currency, float(amount_aud),
             name, clean_name, category, note, tx_type, is_new))
    except Exception as e: log.error(f"save_tx: {e}")

def tx_exists(tx_id):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM wise_tx WHERE id=%s", (str(tx_id),))
        return cur.fetchone() is not None
    except: return False

def save_known(name, clean_name, category, note=""):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO known_recipients(name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(name) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, note=EXCLUDED.note",
            (name, clean_name, category, note))
        # Update all existing transactions with this name
        db.cursor().execute(
            "UPDATE wise_tx SET clean_name=%s, category=%s, note=%s, is_new=FALSE WHERE name=%s",
            (clean_name, category, note, name))
    except Exception as e: log.error(f"save_known: {e}")

def get_known(name):
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name,category,note FROM known_recipients WHERE name=%s", (name,))
        return cur.fetchone()
    except: return None

def db_count():
    db = get_db()
    if not db: return 0, None, None
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM wise_tx")
        return cur.fetchone()
    except: return 0, None, None

def db_categories():
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT COALESCE(category,'Uncategorized'), SUM(amount_aud), COUNT(*)
            FROM wise_tx GROUP BY COALESCE(category,'Uncategorized')
            ORDER BY SUM(amount_aud) DESC
        """)
        return cur.fetchall()
    except: return []

def db_recipients(start=None, end=None):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = """SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*),
                      COALESCE(category,'Uncategorized')
               FROM wise_tx WHERE 1=1"""
        p = []
        if start: q += " AND created_at>=%s"; p.append(start)
        if end:   q += " AND created_at<=%s"; p.append(end)
        q += " GROUP BY COALESCE(clean_name,name),COALESCE(category,'Uncategorized') ORDER BY SUM(amount_aud) DESC LIMIT 30"
        cur.execute(q, p)
        return cur.fetchall()
    except: return []

def db_recent(n=30):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT created_at, amount_aud, currency, amount,
                   COALESCE(clean_name,name), COALESCE(category,'?'), tx_type
            FROM wise_tx ORDER BY created_at DESC LIMIT %s
        """, (n,))
        return cur.fetchall()
    except: return []

# ── WISE ──────────────────────────────────────────────────────────────────────
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

def to_aud(amount, currency):
    if currency == "AUD": return float(amount)
    try:
        r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{currency}", timeout=5)
        return round(float(amount) * r.json()["rates"]["AUD"], 4)
    except:
        rates = {"USD":1.56,"GBP":1.97,"EUR":1.72,"PKR":0.0056,"PHP":0.027}
        return round(float(amount) * rates.get(currency, 1.0), 4)

_name_cache = {}
def get_account_name(account_id):
    if not account_id: return None
    key = str(account_id)
    if key in _name_cache: return _name_cache[key]
    try:
        r = requests.get(f"{WISE_BASE}/v1/accounts/{account_id}",
            headers=wise_h(), timeout=6)
        if r.status_code == 200:
            d = r.json()
            name = (d.get("accountHolderName") or d.get("name") or
                    (d.get("details") or {}).get("accountHolderName"))
            if name: _name_cache[key] = name; return name
    except: pass
    return None

def load_history():
    """Load ALL Wise history silently. No questions about past transactions."""
    pid = get_pid()
    total = 0

    # Bank transfers
    offset = 0
    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(), params={"profile":pid,"limit":100,"offset":offset}, timeout=20)
            r.raise_for_status()
            batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
            if not batch: break
            for tx in batch:
                if tx.get("status") != "outgoing_payment_sent": continue
                tx_id  = str(tx.get("id",""))
                amount = float(tx.get("sourceValue") or 0)
                cur    = tx.get("sourceCurrency","AUD")
                if not tx_id or amount == 0: continue
                aud    = to_aud(amount, cur)
                acct_id = tx.get("targetAccount")
                name   = get_account_name(acct_id) if acct_id else None
                if not name:
                    det = tx.get("details") or {}
                    name = det.get("reference") or f"transfer-{tx_id}"
                date_s = tx.get("created","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                except: d = datetime.now(timezone.utc)
                known = get_known(name)
                clean = known[0] if known else None
                cat   = known[1] if known else None
                note  = known[2] if known else None
                save_tx(tx_id, d, amount, cur, aud, name, clean, cat, note, "TRANSFER", False)
                total += 1
            if len(batch) < 100: break
            offset += 100
        except Exception as e: log.error(f"load_transfers: {e}"); break

    # Card transactions via balance statements
    try:
        br = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
            headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
        br.raise_for_status()
        balances = [(b["id"], b["amount"]["currency"]) for b in br.json() if b.get("id")]
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
                if r.status_code != 200:
                    log.error(f"Statement {currency}: HTTP {r.status_code}")
                    break
                for tx in r.json().get("transactions",[]):
                    ref = str(tx.get("referenceNumber") or "")
                    if not ref.startswith("CARD_TRANSACTION"): continue
                    if tx_exists(ref): continue
                    val = float(tx.get("amount",{}).get("value",0))
                    if val >= 0: continue
                    amount = abs(val)
                    aud    = to_aud(amount, currency)
                    det    = tx.get("details",{}) or {}
                    name   = ""
                    for key in ["merchant","senderName","description"]:
                        v = det.get(key)
                        if v and isinstance(v, str) and v.strip():
                            name = v.strip(); break
                    if not name: continue
                    if name in ("Divisible Inc","LEADS PILOT LLC"): continue
                    date_s = tx.get("date") or tx.get("createdAt","")
                    try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                    except: d = datetime.now(timezone.utc)
                    known = get_known(name)
                    clean = known[0] if known else None
                    cat   = known[1] if known else None
                    note  = known[2] if known else None
                    save_tx(ref, d, amount, currency, aud, name, clean, cat, note, "CARD", False)
                    total += 1
            except Exception as e: log.error(f"stmt chunk: {e}")
            chunk_end = chunk_start

    log.info(f"History loaded: {total} transactions")
    return total

def check_new_transactions():
    """Check for transactions not yet in DB. Returns list of new ones."""
    pid = get_pid()
    new_txs = []

    # New bank transfers
    try:
        r = requests.get(f"{WISE_BASE}/v1/transfers",
            headers=wise_h(), params={"profile":pid,"limit":10,"offset":0}, timeout=20)
        r.raise_for_status()
        batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
        for tx in batch:
            if tx.get("status") != "outgoing_payment_sent": continue
            tx_id  = str(tx.get("id",""))
            if not tx_id or tx_exists(tx_id): continue
            amount = float(tx.get("sourceValue") or 0)
            cur    = tx.get("sourceCurrency","AUD")
            aud    = to_aud(amount, cur)
            acct_id = tx.get("targetAccount")
            name   = get_account_name(acct_id) if acct_id else None
            if not name:
                det = tx.get("details") or {}
                name = det.get("reference") or f"transfer-{tx_id}"
            date_s = tx.get("created","")
            try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
            except: d = datetime.now(timezone.utc)
            save_tx(tx_id, d, amount, cur, aud, name, None, None, None, "TRANSFER", True)
            new_txs.append({"id":tx_id,"name":name,"aud":aud,"cur":cur,"orig":amount,"date":d})
    except Exception as e: log.error(f"check_transfers: {e}")

    # New card transactions (last 2 days)
    try:
        br = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
            headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
        br.raise_for_status()
        balances = [(b["id"], b["amount"]["currency"]) for b in br.json() if b.get("id")]
        now   = datetime.now(timezone.utc)
        start = now - timedelta(days=2)
        for bal_id, currency in balances:
            r = requests.get(
                f"{WISE_BASE}/v1/profiles/{pid}/balance-statements/{bal_id}/statement.json",
                headers=wise_h(),
                params={"currency":currency,
                        "intervalStart":start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "intervalEnd":now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "type":"COMPACT"}, timeout=20)
            if r.status_code != 200: continue
            for tx in r.json().get("transactions",[]):
                ref = str(tx.get("referenceNumber") or "")
                if not ref.startswith("CARD_TRANSACTION"): continue
                if tx_exists(ref): continue
                val = float(tx.get("amount",{}).get("value",0))
                if val >= 0: continue
                amount = abs(val)
                aud    = to_aud(amount, currency)
                det    = tx.get("details",{}) or {}
                name   = ""
                for key in ["merchant","senderName","description"]:
                    v = det.get(key)
                    if v and isinstance(v, str) and v.strip():
                        name = v.strip(); break
                if not name or name in ("Divisible Inc","LEADS PILOT LLC"): continue
                date_s = tx.get("date") or tx.get("createdAt","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00"))
                except: d = datetime.now(timezone.utc)
                save_tx(ref, d, amount, currency, aud, name, None, None, None, "CARD", True)
                new_txs.append({"id":ref,"name":name,"aud":aud,"cur":currency,"orig":amount,"date":d})
    except Exception as e: log.error(f"check_cards: {e}")

    return new_txs

# ── PENDING CONTEXT ────────────────────────────────────────────────────────────
_pending = {}  # tx_id -> tx info

def notify_new_tx(tx, client):
    """Ask Suleman about a new transaction."""
    name = tx["name"]
    aud  = tx["aud"]
    cur  = tx["cur"]
    orig = tx["orig"]
    d    = tx["date"]
    tx_id = tx["id"]

    # Check if already known
    known = get_known(name)
    if known:
        clean, cat, note = known
        save_tx(tx_id, d, orig, cur, aud, name, clean, cat, note,
                "CARD" if "CARD" in tx_id else "TRANSFER", False)
        est = d.astimezone(EST).strftime("%b %d %I:%M %p EST")
        amt_str = f"{aud:,.2f} AUD"
        if cur != "AUD": amt_str += f" ({orig:,.2f} {cur})"
        try:
            client.chat_postMessage(channel=CHANNEL_ID,
                text=f":white_check_mark: *{est}* — {amt_str} to *{clean}* [{cat}]")
        except: pass
        return

    # Unknown — ask Suleman
    _pending[tx_id] = tx
    est = d.astimezone(EST).strftime("%b %d %I:%M %p EST")
    amt_str = f"{aud:,.2f} AUD"
    if cur != "AUD": amt_str += f" ({orig:,.2f} {cur})"
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text=f":question: *New transaction — {est}*\n"
                 f"You sent *{amt_str}* to *`{name}`*\n\n"
                 f"Who is this and what's it for?")
    except Exception as e: log.error(f"notify: {e}")

# ── CLAUDE ────────────────────────────────────────────────────────────────────
def claude_ask(prompt, system="", max_tokens=300):
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":max_tokens,
                  "system":system,"messages":[{"role":"user","content":prompt}]},
            timeout=20)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"claude: {e}")
    return None

def claude_extract_info(user_reply, tx_name, amount_aud):
    """Extract clean name, category, note from user's natural language reply."""
    system = """You are an accountant assistant. Extract info from the user's reply about a transaction.
Return JSON only (no markdown):
{"clean_name": "readable name", "category": "category", "note": "brief description"}

Categories to use:
- SMS Cost (Signal House, Sendivo etc)
- Data Provider (Fanbasis, Zeeshan etc)
- Salary (staff payments)
- Software (Anthropic, Zoom, Slack, HighLevel etc)
- Personal (personal expenses)
- Rent (house/office rent)
- Investment (stocks, brokers)
- Hardware (phones, computers)
- Loan/Personal (loans, pass-throughs)
- Business Other (anything else business related)

Be smart — infer from context. If they say "that's our SMS provider" use SMS Cost etc."""

    result = claude_ask(
        f"Transaction to: {tx_name}\nAmount: {amount_aud:.2f} AUD\nUser says: {user_reply}",
        system=system)
    if result:
        try:
            return json.loads(result)
        except: pass
    return None

def answer_question(q):
    """Claude answers questions about spending."""
    now   = datetime.now(EST)
    today = now.date()
    month_start = today.replace(day=1)

    try: bals = get_balances()
    except: bals = []

    count, oldest, newest = db_count()
    cats   = db_categories()
    recips = db_recipients()
    recips_month = db_recipients(
        datetime(today.year, today.month, 1, tzinfo=timezone.utc),
        datetime.now(timezone.utc))
    recent = db_recent(30)

    bal_text = "\n".join(f"  {b}" for b in bals) or "unavailable"

    def fmt_row(r):
        est = r[0].astimezone(EST).strftime("%b %d %I:%M %p EST") if r[0] else "?"
        aud = float(r[1])
        cur = r[2]
        orig = float(r[3])
        name = r[4]
        cat  = r[5]
        s = f"  {est}: {aud:,.2f} AUD"
        if cur != "AUD": s += f" ({orig:,.2f} {cur})"
        s += f" -> {name} [{cat}]"
        return s

    cat_text    = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]} txns)" for r in cats)
    recip_text  = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]} txns)" for r in recips)
    month_text  = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD" for r in recips_month)
    recent_text = "\n".join(fmt_row(r) for r in recent)

    system = f"""You are the Wise accountant for LeadsPilot — Suleman's SMS lead gen business (Australia).
Answer directly. All amounts in AUD. Times in EST.
Today: {now.strftime("%b %d %Y %I:%M %p EST")}
This month: {month_start}

WISE BALANCES:
{bal_text}

DB: {count} transactions ({oldest} to {newest} if oldest else "none")

ALL-TIME BY CATEGORY:
{cat_text}

ALL-TIME BY RECIPIENT:
{recip_text}

THIS MONTH BY RECIPIENT:
{month_text}

RECENT TRANSACTIONS:
{recent_text}"""

    return claude_ask(q, system=system, max_tokens=500) or "Could not answer."

# ── WEBHOOK ───────────────────────────────────────────────────────────────────
def register_webhook():
    try:
        pid = get_pid()
        url = f"{RAILWAY_URL}/webhook/wise"
        h   = {**wise_h(), "Content-Type":"application/json"}
        r   = requests.get(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions", headers=h, timeout=10)
        existing = []
        if r.status_code == 200:
            existing = [s.get("trigger_on","") for s in r.json()]
        if "transfers#state-change" not in existing:
            r2 = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
                headers=h, timeout=10,
                json={"name":"LeadsPilot Bot","trigger_on":"transfers#state-change",
                      "delivery":{"version":"2.0.0","url":url}})
            log.info(f"Webhook registered: HTTP {r2.status_code}")
        else:
            log.info("Webhook already active")
    except Exception as e: log.error(f"webhook reg: {e}")

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Downloading all Wise transaction history..._")
        register_webhook()
        n = load_history()
        count, oldest, newest = db_count()
        bals  = get_balances()
        cats  = db_categories()
        oldest_str = oldest.astimezone(EST).strftime("%b %d %Y") if oldest else "?"
        newest_str = newest.astimezone(EST).strftime("%b %d %Y") if newest else "?"
        msg  = f"*Wise Accountant Bot ready* :white_check_mark:\n"
        msg += f"*{count} transactions loaded* ({oldest_str} → {newest_str})\n"
        msg += "*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if cats:
            msg += "\n\n*All-time spending by category:*\n"
            for cat, total, cnt in cats:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt} payments)\n"
        msg += "\nI'll ask you about every new transaction going forward. Ask me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

# ── WORKER ────────────────────────────────────────────────────────────────────
def worker(client):
    while True:
        time.sleep(180)
        try:
            new_txs = check_new_transactions()
            for tx in new_txs:
                notify_new_tx(tx, client)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    count, _, _ = db_count()
    return jsonify({"status":"ok","transactions":count})

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status":"ok"}), 200
    try:
        payload = request.get_json(force=True) or {}
        log.info(f"Webhook: {payload.get('event_type','')}")
        def handle():
            new_txs = check_new_transactions()
            for tx in new_txs:
                notify_new_tx(tx, slack_app.client)
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
    if _pending:
        # Try to match reply to any pending transaction
        for tx_id, tx in list(_pending.items()):
            info = claude_extract_info(text, tx["name"], tx["aud"])
            if info and info.get("category"):
                clean = info.get("clean_name", tx["name"])
                cat   = info.get("category","Unknown")
                note  = info.get("note","")
                # Save to DB
                save_tx(tx_id, tx["date"], tx["orig"], tx["cur"], tx["aud"],
                        tx["name"], clean, cat, note,
                        "CARD" if "CARD" in tx_id else "TRANSFER", False)
                # Remember for future
                save_known(tx["name"], clean, cat, note)
                del _pending[tx_id]
                say(f":white_check_mark: Got it! *{clean}* — {cat}"
                    + (f"\n_{note}_" if note else ""))
                return

    # Otherwise answer as accountant
    say("_Checking..._")
    say(answer_question(text))

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
