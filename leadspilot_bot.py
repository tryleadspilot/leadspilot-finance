"""
Wise Accountant Bot — Bulletproof Final Version
Rules:
- Load CSV on startup -> SILENT. No questions. Just post 1 summary.
- New transactions going forward -> notify in Slack
- Never ask about past transactions
- All amounts in AUD
"""
import os, re, csv, io, json, logging, requests, threading, time, ssl, urllib.parse
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
        DROP TABLE IF EXISTS tx;
        DROP TABLE IF EXISTS known;
        CREATE TABLE tx (
            id          TEXT PRIMARY KEY,
            date        DATE NOT NULL,
            amount_aud  NUMERIC(14,4) NOT NULL,
            currency    TEXT DEFAULT 'AUD',
            amount_orig NUMERIC(14,4),
            name        TEXT,
            clean_name  TEXT,
            category    TEXT,
            tx_type     TEXT,
            is_new      BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE known (
            name       TEXT PRIMARY KEY,
            clean_name TEXT,
            category   TEXT,
            note       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tx_date ON tx(date);
        CREATE INDEX IF NOT EXISTS idx_tx_new  ON tx(is_new);
        CREATE INDEX IF NOT EXISTS idx_tx_cat  ON tx(category);
    """)
    log.info("DB ready")

def save_tx(tx_id, date, aud, currency, orig, name, clean, cat, tx_type, is_new=False):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO tx(id,date,amount_aud,currency,amount_orig,name,clean_name,category,tx_type,is_new) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET "
            "clean_name=COALESCE(EXCLUDED.clean_name, tx.clean_name),"
            "category=COALESCE(EXCLUDED.category, tx.category),"
            "is_new=CASE WHEN tx.is_new=FALSE THEN FALSE ELSE EXCLUDED.is_new END",
            (tx_id, date, float(aud), currency, float(orig) if orig else float(aud),
             name, clean, cat, tx_type, is_new))
    except Exception as e: log.error(f"save_tx {tx_id}: {e}")

def tx_exists(tx_id):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM tx WHERE id=%s", (str(tx_id),))
        return cur.fetchone() is not None
    except: return False

def get_known(name):
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name,category,note FROM known WHERE name=%s", (name,))
        return cur.fetchone()
    except: return None

def save_known(name, clean, cat, note=""):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO known(name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(name) DO UPDATE SET clean_name=EXCLUDED.clean_name,"
            "category=EXCLUDED.category,note=EXCLUDED.note",
            (name, clean, cat, note))
        db.cursor().execute(
            "UPDATE tx SET clean_name=%s,category=%s,is_new=FALSE WHERE name=%s",
            (clean, cat, name))
    except Exception as e: log.error(f"save_known: {e}")

def to_aud(amount, cur):
    if not amount or float(amount) == 0: return 0.0
    if cur == "AUD": return round(float(amount), 4)
    try:
        r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{cur}", timeout=5)
        return round(float(amount) * r.json()["rates"]["AUD"], 4)
    except:
        return round(float(amount) * {
            "USD":1.56,"GBP":1.97,"EUR":1.72,"PKR":0.0056,"PHP":0.027
        }.get(cur, 1.0), 4)

def categorize(name):
    """Smart categorize — no API call needed."""
    n = name.lower()
    if any(x in n for x in ["signal house"]): return "Signal House SMS", "SMS Cost"
    if any(x in n for x in ["sendivo"]): return "Sendivo", "SMS Cost"
    if any(x in n for x in ["fanbasis"]): return "Fanbasis (Jacob)", "Data Provider"
    if any(x in n for x in ["zeeshan","ghulam shabir"]): return "Zeeshan Shabbir", "Data Provider"
    if any(x in n for x in ["anthropic","claude"]): return name, "Software"
    if any(x in n for x in ["highlevel","high level"]): return "HighLevel", "Software"
    if any(x in n for x in ["zoom"]): return "Zoom", "Software"
    if any(x in n for x in ["slack"]): return "Slack", "Software"
    if any(x in n for x in ["calendly"]): return "Calendly", "Software"
    if any(x in n for x in ["n8n"]): return "N8n Cloud", "Software"
    if any(x in n for x in ["retell"]): return "Retell AI", "Software"
    if any(x in n for x in ["instantly"]): return "Instantly", "Software"
    if any(x in n for x in ["framer"]): return "Framer", "Software"
    if any(x in n for x in ["google"]): return "Google Workspace", "Software"
    if any(x in n for x in ["opus virtual"]): return "Opus Virtual Offices", "Software"
    if any(x in n for x in ["grasshopper"]): return "Grasshopper", "Software"
    if any(x in n for x in ["onlinejobs","onlinejobsph"]): return "OnlineJobs.ph", "Software"
    if any(x in n for x in ["bizee"]): return "Bizee", "Software"
    if any(x in n for x in ["whop"]): return "Whop", "Software"
    if any(x in n for x in ["airbnb"]): return "Airbnb", "Personal"
    if any(x in n for x in ["mulebuy"]): return "Mulebuy", "Personal"
    if any(x in n for x in ["interactive brokers"]): return "Interactive Brokers", "Investment"
    if any(x in n for x in ["usman ahmed"]): return "Usman Ahmed", "Rent"
    if any(x in n for x in ["wahaj","shayan amir"]): return "Wahaj Khan", "Loan/Personal"
    if any(x in n for x in ["muhammad hisham"]): return "Muhammad Hisham", "Personal"
    if any(x in n for x in ["abdul rehman tahir"]): return "Abdul Rehman Tahir", "Hardware"
    if any(x in n for x in ["abdul rehman"]): return "Abdul Rehman", "Personal"
    if any(x in n for x in ["starla","nina","queenzen","john isaac","annalyn","budejas","cayongcong","pamolarco"]): return name, "Salary"
    if any(x in n for x in ["moeez mazhar","pak mac"]): return name, "Hardware"
    if any(x in n for x in ["rinip","saurabh","abdullah habib","inyxel"]): return name, "Business Other"
    return name, "Unknown"

# ── CSV IMPORT ────────────────────────────────────────────────────────────────
def import_csv(text):
    """
    Import Wise CSV export.
    ONLY COMPLETED OUT transactions.
    NEVER marks as is_new — these are historical.
    Uses Target name from CSV (real names like 'Signal House SMS').
    """
    reader = csv.DictReader(io.StringIO(text))
    count = 0
    for row in reader:
        if row.get("Status","").strip() != "COMPLETED": continue
        if row.get("Direction","").strip() != "OUT": continue
        tx_id = row.get("ID","").strip()
        name  = row.get("Target name","").strip()
        date_s = row.get("Created on","").strip()[:10]
        if not tx_id or not name or not date_s: continue
        if name in ("Divisible Inc","LEADS PILOT LLC"): continue

        src_amt = float(row.get("Source amount (after fees)","0") or 0)
        src_cur = row.get("Source currency","AUD").strip()
        tgt_amt = float(row.get("Target amount (after fees)","0") or 0)
        tgt_cur = row.get("Target currency","AUD").strip()

        # Use source AUD amount (most accurate for what left your account)
        aud  = to_aud(src_amt, src_cur)
        orig = src_amt
        cur  = src_cur
        if aud == 0: continue

        try: date = datetime.strptime(date_s, "%Y-%m-%d").date()
        except: continue

        tx_type = "CARD" if "CARD_TRANSACTION" in tx_id else "TRANSFER"

        # Get or create categorization
        known = get_known(name)
        if known:
            clean, cat = known[0], known[1]
        else:
            clean, cat = categorize(name)
            save_known(name, clean, cat)

        # is_new=FALSE — this is history, never ask about it
        save_tx(tx_id, date, aud, cur, orig, name, clean, cat, tx_type, False)
        count += 1

    log.info(f"CSV imported: {count} transactions")
    return count

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
def resolve_name(account_id):
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

def check_new():
    """Check Wise API for NEW bank transfers not in DB."""
    pid = get_pid()
    new_txs = []
    try:
        r = requests.get(f"{WISE_BASE}/v1/transfers",
            headers=wise_h(), params={"profile":pid,"limit":20,"offset":0}, timeout=20)
        r.raise_for_status()
        batch = r.json() if isinstance(r.json(), list) else r.json().get("content",[])
        for tx in batch:
            if tx.get("status") != "outgoing_payment_sent": continue
            tx_id = str(tx.get("id",""))
            if not tx_id or tx_exists(tx_id): continue
            amount = float(tx.get("sourceValue") or 0)
            cur    = tx.get("sourceCurrency","AUD")
            aud    = to_aud(amount, cur)
            acct_id = tx.get("targetAccount")
            name = resolve_name(acct_id) if acct_id else None
            if not name:
                det = tx.get("details") or {}
                name = det.get("reference") or f"transfer-{tx_id}"
            date_s = tx.get("created","")
            try: d = datetime.fromisoformat(date_s.replace("Z","+00:00")).date()
            except: d = datetime.now().date()
            known = get_known(name)
            if known:
                clean, cat = known[0], known[1]
                save_tx(tx_id, d, aud, cur, amount, name, clean, cat, "TRANSFER", False)
            else:
                clean, cat = categorize(name)
                if cat != "Unknown":
                    save_tx(tx_id, d, aud, cur, amount, name, clean, cat, "TRANSFER", False)
                    save_known(name, clean, cat)
                else:
                    save_tx(tx_id, d, aud, cur, amount, name, None, None, "TRANSFER", True)
            new_txs.append({"id":tx_id,"name":name,"clean":clean,"cat":cat,
                            "aud":aud,"cur":cur,"orig":amount,"date":d,
                            "known": cat != "Unknown"})
    except Exception as e: log.error(f"check_new: {e}")
    return new_txs

def register_webhook():
    try:
        pid = get_pid()
        url = f"{RAILWAY_URL}/webhook/wise"
        h   = {**wise_h(), "Content-Type":"application/json"}
        r   = requests.get(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions", headers=h, timeout=10)
        existing = [s.get("trigger_on","") for s in (r.json() if r.status_code==200 else [])]
        if "transfers#state-change" not in existing:
            r2 = requests.post(f"{WISE_BASE}/v3/profiles/{pid}/subscriptions",
                headers=h, timeout=10,
                json={"name":"LeadsPilot","trigger_on":"transfers#state-change",
                      "delivery":{"version":"2.0.0","url":url}})
            log.info(f"Webhook: HTTP {r2.status_code} — {r2.text[:80]}")
        else:
            log.info("Webhook already registered")
    except Exception as e: log.error(f"webhook: {e}")

# ── NOTIFY ────────────────────────────────────────────────────────────────────
_pending = {}

def notify(tx, client):
    tx_id = tx["id"]
    name  = tx["name"]
    clean = tx.get("clean") or name
    cat   = tx.get("cat","")
    aud   = tx["aud"]
    cur   = tx["cur"]
    orig  = tx["orig"]
    d     = tx["date"]
    known = tx.get("known", False)
    amt   = f"{aud:,.2f} AUD" + (f" ({orig:,.2f} {cur})" if cur != "AUD" else "")

    if known:
        try:
            client.chat_postMessage(channel=CHANNEL_ID,
                text=f":white_check_mark: *New transaction — {d}*\n"
                     f"{amt} → *{clean}* [{cat}]")
        except: pass
    else:
        _pending[tx_id] = tx
        try:
            client.chat_postMessage(channel=CHANNEL_ID,
                text=f":question: *New transaction — {d}*\n"
                     f"You sent *{amt}* to *{name}*\n"
                     f"Who is this?")
        except Exception as e: log.error(f"notify: {e}")

# ── DB QUERIES ────────────────────────────────────────────────────────────────
def summary():
    db = get_db()
    if not db: return {}
    try:
        now   = datetime.now(EST)
        month = now.date().replace(day=1)
        cur   = db.cursor()
        cur.execute("SELECT COUNT(*), SUM(amount_aud), MIN(date), MAX(date) FROM tx")
        count, total, oldest, newest = cur.fetchone()
        cur.execute("SELECT SUM(amount_aud) FROM tx WHERE date>=%s", (month,))
        month_total = float((cur.fetchone() or [0])[0] or 0)
        cur.execute("""
            SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*)
            FROM tx GROUP BY COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC
        """)
        cats = cur.fetchall()
        return {"count":count,"total":float(total or 0),"oldest":oldest,
                "newest":newest,"month_total":month_total,"cats":cats}
    except: return {}

# ── CLAUDE ANSWER ─────────────────────────────────────────────────────────────
def claude(prompt, system, max_tokens=500):
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01",
                     "content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":max_tokens,
                  "system":system,"messages":[{"role":"user","content":prompt}]},
            timeout=20)
        if r.status_code == 200: return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"claude: {e}")
    return None

def answer(q):
    s = summary()
    try: bals = get_balances()
    except: bals = []
    db = get_db()
    recips = []; recent = []
    if db:
        cur = db.cursor()
        cur.execute("""
            SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*),
                   COALESCE(category,'Unknown')
            FROM tx GROUP BY COALESCE(clean_name,name),COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC LIMIT 25
        """)
        recips = cur.fetchall()
        # This month
        month = datetime.now(EST).date().replace(day=1)
        cur.execute("""
            SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*),
                   COALESCE(category,'Unknown')
            FROM tx WHERE date>=%s
            GROUP BY COALESCE(clean_name,name),COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC LIMIT 25
        """, (month,))
        month_recips = cur.fetchall()
        cur.execute("""
            SELECT date, amount_aud, currency, amount_orig,
                   COALESCE(clean_name,name), COALESCE(category,'?')
            FROM tx ORDER BY date DESC LIMIT 30
        """)
        recent = cur.fetchall()

    bal_text = "\n".join(f"  {b}" for b in bals) or "unavailable"
    cat_text = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in (s.get("cats") or []))
    rec_text = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips)
    month_text = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD" for r in month_recips)
    recent_text = "\n".join(
        f"  {r[0]}: {float(r[1]):,.2f} AUD"
        + (f" ({float(r[3]):,.2f} {r[2]})" if r[2] != "AUD" else "")
        + f" -> {r[4]} [{r[5]}]"
        for r in recent)

    system_prompt = f"""You are the Wise accountant for LeadsPilot — Suleman's SMS lead gen business.
Answer directly. All amounts in AUD. Today: {datetime.now(EST).strftime("%b %d %Y")}

BALANCES: {bal_text}
TOTAL TRANSACTIONS: {s.get("count",0)} ({s.get("oldest","")} to {s.get("newest","")})
THIS MONTH TOTAL: {s.get("month_total",0):,.2f} AUD
ALL TIME TOTAL: {s.get("total",0):,.2f} AUD

ALL TIME BY CATEGORY:
{cat_text}

ALL TIME BY RECIPIENT:
{rec_text}

THIS MONTH BY RECIPIENT:
{month_text}

RECENT TRANSACTIONS:
{recent_text}"""

    return claude(q, system_prompt) or "Could not answer right now."

def extract_info(text, tx_name, aud):
    result = claude(
        f"Transaction to: {tx_name}\nAmount: {aud:.2f} AUD\nUser reply: {text}",
        """Extract recipient info. Return JSON only:
{"clean_name":"readable name","category":"category","note":"what it is"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Rent, Investment, Hardware, Loan/Personal, Business Other
Be smart from context.""")
    if result:
        try: return json.loads(result)
        except: pass
    return None

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Loading Wise transaction history..._")
        register_webhook()
        # Load CSV
        n = 0
        for path in ["transaction-history.csv", "/app/transaction-history.csv"]:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    n = import_csv(f.read())
                log.info(f"Loaded CSV: {n} transactions from {path}")
                break
        if n == 0:
            log.warning("No CSV found — only API data available")
        s    = summary()
        bals = get_balances()
        msg  = f"*Wise Bot ready* :white_check_mark:\n"
        msg += f"*{s.get('count',0)} transactions loaded*"
        if s.get("oldest"): msg += f" ({s['oldest']} → {s['newest']})"
        msg += f"\n*This month:* {s.get('month_total',0):,.2f} AUD"
        msg += f"\n*All time:* {s.get('total',0):,.2f} AUD"
        msg += "\n*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if s.get("cats"):
            msg += "\n\n*By category (all time):*\n"
            for cat, total, cnt in s["cats"]:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        msg += "\nI'll notify you about every new transaction. Ask me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(180)
        try:
            new_txs = check_new()
            for tx in new_txs:
                notify(tx, client)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    s = summary()
    return jsonify({"status":"ok","transactions":s.get("count",0)})

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status":"ok"}), 200
    try:
        payload = request.get_json(force=True) or {}
        log.info(f"Webhook: {payload.get('event_type','')}")
        def handle():
            new_txs = check_new()
            for tx in new_txs: notify(tx, slack_app.client)
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
    # Reply about pending new transaction
    if _pending:
        for tx_id, tx in list(_pending.items()):
            info = extract_info(text, tx["name"], tx["aud"])
            if info and info.get("category"):
                clean = info.get("clean_name", tx["name"])
                cat   = info.get("category","Unknown")
                note  = info.get("note","")
                save_known(tx["name"], clean, cat, note)
                del _pending[tx_id]
                say(f":white_check_mark: *{clean}* — {cat}" +
                    (f"\n_{note}_" if note else ""))
                return
    say("_Checking..._")
    say(answer(text))

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text",""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
    # Handle CSV dropped in Slack
    for f in event.get("files",[]):
        if f.get("name","").endswith(".csv"):
            url = f.get("url_private") or f.get("url_private_download")
            if url:
                r = requests.get(url,
                    headers={"Authorization":f"Bearer {SLACK_BOT_TOKEN}"}, timeout=15)
                if r.status_code == 200:
                    n = import_csv(r.text)
                    s = summary()
                    say(f":white_check_mark: *{n} transactions loaded*\n"
                        f"Total: {s.get('count',0)} | "
                        f"This month: {s.get('month_total',0):,.2f} AUD | "
                        f"All time: {s.get('total',0):,.2f} AUD")
                return
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
