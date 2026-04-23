"""
Wise Accountant Bot - Simple & Working
1. Load CSV on startup silently -> post one summary
2. New transactions -> notify Suleman -> he tells bot who it is
3. Webhook for live notifications
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
        DROP TABLE IF EXISTS wise_tx;
        DROP TABLE IF EXISTS recipients;
        CREATE TABLE wise_tx (
            id         TEXT PRIMARY KEY,
            date       DATE NOT NULL,
            amount_aud NUMERIC(14,4),
            currency   TEXT,
            amount_orig NUMERIC(14,4),
            name       TEXT,
            clean_name TEXT,
            category   TEXT,
            note       TEXT,
            tx_type    TEXT,
            is_new     BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE recipients (
            name       TEXT PRIMARY KEY,
            clean_name TEXT,
            category   TEXT,
            note       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_date ON wise_tx(date);
        CREATE INDEX IF NOT EXISTS idx_new  ON wise_tx(is_new);
    """)
    log.info("DB ready")

def upsert(tx_id, date, amount_aud, currency, amount_orig, name, clean_name=None, category=None, note=None, tx_type="CARD", is_new=False):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO wise_tx(id,date,amount_aud,currency,amount_orig,name,clean_name,category,note,tx_type,is_new) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=COALESCE(EXCLUDED.clean_name,wise_tx.clean_name),"
            "category=COALESCE(EXCLUDED.category,wise_tx.category)",
            (tx_id, date, float(amount_aud), currency, float(amount_orig),
             name, clean_name, category, note, tx_type, is_new))
    except Exception as e: log.error(f"upsert: {e}")

def exists(tx_id):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM wise_tx WHERE id=%s", (str(tx_id),))
        return cur.fetchone() is not None
    except: return False

def get_recipient(name):
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name,category,note FROM recipients WHERE name=%s", (name,))
        return cur.fetchone()
    except: return None

def save_recipient(name, clean_name, category, note=""):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO recipients(name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(name) DO UPDATE SET clean_name=EXCLUDED.clean_name,"
            "category=EXCLUDED.category,note=EXCLUDED.note",
            (name, clean_name, category, note))
        db.cursor().execute(
            "UPDATE wise_tx SET clean_name=%s,category=%s,note=%s,is_new=FALSE WHERE name=%s",
            (clean_name, category, note, name))
    except Exception as e: log.error(f"save_recipient: {e}")

def to_aud(amount, currency):
    if currency == "AUD": return float(amount)
    try:
        r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{currency}", timeout=5)
        return round(float(amount) * r.json()["rates"]["AUD"], 4)
    except:
        return round(float(amount) * {"USD":1.56,"GBP":1.97,"EUR":1.72,"PKR":0.0056,"PHP":0.027}.get(currency,1.0), 4)

# ── CSV IMPORT ────────────────────────────────────────────────────────────────
def import_csv(text):
    """Import Wise CSV export. Only COMPLETED OUT transactions."""
    reader = csv.DictReader(io.StringIO(text))
    count = 0
    for row in reader:
        if row.get("Status","").strip() != "COMPLETED": continue
        if row.get("Direction","").strip() != "OUT": continue
        tx_id    = row.get("ID","").strip()
        raw_name = row.get("Target name","").strip()
        src_amt  = row.get("Source amount (after fees)","0").strip() or "0"
        src_cur  = row.get("Source currency","AUD").strip()
        tgt_amt  = row.get("Target amount (after fees)","0").strip() or "0"
        tgt_cur  = row.get("Target currency","AUD").strip()
        date_s   = row.get("Created on","").strip()[:10]
        if not tx_id or not raw_name: continue
        # Use target amount if USD (more accurate), else source
        if tgt_cur == "USD":
            amount_orig = float(tgt_amt or 0)
            currency = "USD"
            amount_aud = to_aud(amount_orig, "USD")
        else:
            amount_orig = float(src_amt or 0)
            currency = src_cur
            amount_aud = to_aud(amount_orig, currency)
        if amount_aud == 0: continue
        try: date = datetime.strptime(date_s, "%Y-%m-%d").date()
        except: continue
        # Skip incoming self-transfers
        if raw_name in ("Divisible Inc", "LEADS PILOT LLC"): continue
        known = get_recipient(raw_name)
        upsert(tx_id, date, amount_aud, currency, amount_orig, raw_name,
               known[0] if known else None,
               known[1] if known else None,
               known[2] if known else None,
               "CARD" if "CARD_TRANSACTION" in tx_id else "TRANSFER")
        count += 1
    log.info(f"CSV import: {count} transactions")
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

def check_new_transfers():
    """Check Wise API for new bank transfers not in DB."""
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
            if not tx_id or exists(tx_id): continue
            amount = float(tx.get("sourceValue") or 0)
            cur    = tx.get("sourceCurrency","AUD")
            aud    = to_aud(amount, cur)
            acct_id = tx.get("targetAccount")
            name = get_account_name(acct_id) if acct_id else None
            if not name:
                det = tx.get("details") or {}
                name = det.get("reference") or f"transfer-{tx_id}"
            date_s = tx.get("created","")
            try: d = datetime.fromisoformat(date_s.replace("Z","+00:00")).date()
            except: d = datetime.now().date()
            known = get_recipient(name)
            upsert(tx_id, d, aud, cur, amount, name,
                   known[0] if known else None,
                   known[1] if known else None,
                   known[2] if known else None,
                   "TRANSFER", not bool(known))
            if not known:
                new_txs.append({"id":tx_id,"name":name,"aud":aud,"cur":cur,"orig":amount,"date":d})
    except Exception as e: log.error(f"check_transfers: {e}")
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
            log.info(f"Webhook: HTTP {r2.status_code}")
        else:
            log.info("Webhook already registered")
    except Exception as e: log.error(f"webhook: {e}")

# ── SUMMARY ───────────────────────────────────────────────────────────────────
def get_summary():
    db = get_db()
    if not db: return {}
    try:
        cur = db.cursor()
        now   = datetime.now(EST)
        month = now.date().replace(day=1)
        # All time
        cur.execute("SELECT COUNT(*), SUM(amount_aud), MIN(date), MAX(date) FROM wise_tx")
        count, total, oldest, newest = cur.fetchone()
        # This month
        cur.execute("SELECT SUM(amount_aud) FROM wise_tx WHERE date>=%s", (month,))
        month_total = cur.fetchone()[0] or 0
        # By category all time
        cur.execute("""
            SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*)
            FROM wise_tx GROUP BY COALESCE(category,'Unknown')
            ORDER BY SUM(amount_aud) DESC
        """)
        cats = cur.fetchall()
        return {
            "count": count, "total": float(total or 0),
            "oldest": oldest, "newest": newest,
            "month_total": float(month_total),
            "cats": cats, "month": month
        }
    except Exception as e:
        log.error(f"summary: {e}"); return {}

# ── PENDING ───────────────────────────────────────────────────────────────────
_pending = {}

def notify_new(tx, client):
    name = tx["name"]
    aud  = tx["aud"]
    cur  = tx["cur"]
    orig = tx["orig"]
    d    = tx["date"]
    tx_id = tx["id"]
    est_d = datetime.combine(d, datetime.min.time()).astimezone(EST).strftime("%b %d, %Y")
    amt   = f"{aud:,.2f} AUD" + (f" ({orig:,.2f} {cur})" if cur != "AUD" else "")
    _pending[tx_id] = tx
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text=f":question: *New transaction — {est_d}*\n"
                 f"You sent *{amt}* to *`{name}`*\n"
                 f"Who is this and what's it for?")
    except Exception as e: log.error(f"notify: {e}")

# ── CLAUDE ────────────────────────────────────────────────────────────────────
def claude(prompt, system, max_tokens=400):
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":max_tokens,"system":system,
                  "messages":[{"role":"user","content":prompt}]},timeout=20)
        if r.status_code == 200: return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"claude: {e}")
    return None

def extract_recipient_info(text, tx_name, amount_aud):
    result = claude(
        f"Transaction to: {tx_name}\nAmount: {amount_aud:.2f} AUD\nUser says: {text}",
        system="""Extract info from user's reply. Return JSON only:
{"clean_name":"readable name","category":"category","note":"what it is"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Rent, Investment, Hardware, Loan/Personal, Business Other
Be smart — infer from context.""")
    if result:
        try: return json.loads(result)
        except: pass
    return None

def answer(q):
    s = get_summary()
    try: bals = get_balances()
    except: bals = []
    db = get_db()
    recents = []
    by_recip = []
    if db:
        cur = db.cursor()
        cur.execute("""
            SELECT date, amount_aud, currency, amount_orig, COALESCE(clean_name,name), COALESCE(category,'?')
            FROM wise_tx ORDER BY date DESC LIMIT 30
        """)
        recents = cur.fetchall()
        cur.execute("""
            SELECT COALESCE(clean_name,name), SUM(amount_aud), COUNT(*), COALESCE(category,'?')
            FROM wise_tx GROUP BY COALESCE(clean_name,name),COALESCE(category,'?')
            ORDER BY SUM(amount_aud) DESC LIMIT 25
        """)
        by_recip = cur.fetchall()

    bal_text = "\n".join(f"  {b}" for b in bals)
    cat_text = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in (s.get("cats") or []))
    rec_text = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in by_recip)
    recent_text = "\n".join(
        f"  {r[0]}: {float(r[1]):,.2f} AUD" +
        (f" ({float(r[3]):,.2f} {r[2]})" if r[2] != "AUD" else "") +
        f" -> {r[4]} [{r[5]}]"
        for r in recents)

    system = f"""You are the Wise accountant for LeadsPilot — Suleman's SMS lead gen business.
Answer directly. All amounts in AUD. Times in EST.
Today: {datetime.now(EST).strftime("%b %d %Y")}
This month: {s.get("month","")}

BALANCES: {bal_text}
DB: {s.get("count",0)} transactions ({s.get("oldest","")} to {s.get("newest","")})
This month total: {s.get("month_total",0):,.2f} AUD
All time total: {s.get("total",0):,.2f} AUD

BY CATEGORY: {cat_text}
BY RECIPIENT: {rec_text}
RECENT: {recent_text}"""

    return claude(q, system, 500) or "Could not answer."

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Loading Wise transaction history..._")
        register_webhook()
        # Load CSV if present
        n = 0
        for path in ["transaction-history.csv", "/app/transaction-history.csv"]:
            if os.path.exists(path):
                with open(path) as f: n = import_csv(f.read())
                break
        s    = get_summary()
        bals = get_balances()
        msg  = f"*Wise Bot ready* :white_check_mark:\n"
        msg += f"*{s.get('count',0)} transactions loaded* "
        if s.get("oldest"): msg += f"({s['oldest']} to {s['newest']})\n"
        msg += f"*This month:* {s.get('month_total',0):,.2f} AUD\n"
        msg += f"*All time:* {s.get('total',0):,.2f} AUD\n"
        msg += "*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if s.get("cats"):
            msg += "\n\n*All-time by category:*\n"
            for cat, total, cnt in s["cats"]:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        msg += "\nI'll notify you about every new transaction going forward. Ask me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(180)
        try:
            new_txs = check_new_transfers()
            for tx in new_txs:
                notify_new(tx, client)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    s = get_summary()
    return jsonify({"status":"ok","transactions":s.get("count",0)})

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status":"ok"}), 200
    try:
        payload = request.get_json(force=True) or {}
        log.info(f"Webhook: {payload.get('event_type','')}")
        def handle():
            new_txs = check_new_transfers()
            for tx in new_txs: notify_new(tx, slack_app.client)
        threading.Thread(target=handle, daemon=True).start()
        return jsonify({"status":"ok"}), 200
    except Exception as e:
        log.error(f"webhook: {e}"); return jsonify({"status":"error"}), 500

@flask_app.route("/import-csv", methods=["POST"])
def import_csv_endpoint():
    """Upload CSV via HTTP POST to load transaction history."""
    try:
        text = request.data.decode("utf-8")
        n = import_csv(text)
        return jsonify({"status":"ok","imported":n})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)}), 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>","",text or "").strip()
    if not text: return
    # Reply about pending transaction
    if _pending:
        for tx_id, tx in list(_pending.items()):
            info = extract_recipient_info(text, tx["name"], tx["aud"])
            if info and info.get("category") and info.get("category") != "Unknown":
                clean = info.get("clean_name", tx["name"])
                cat   = info.get("category")
                note  = info.get("note","")
                save_recipient(tx["name"], clean, cat, note)
                del _pending[tx_id]
                say(f":white_check_mark: *{clean}* — {cat}" + (f"\n_{note}_" if note else ""))
                return
    # Accountant question
    say("_Checking..._")
    say(answer(text))

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text",""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
    # Handle CSV file uploads in Slack
    for f in event.get("files",[]):
        if f.get("name","").endswith(".csv"):
            url = f.get("url_private") or f.get("url_private_download")
            if url:
                r = requests.get(url, headers={"Authorization":f"Bearer {SLACK_BOT_TOKEN}"}, timeout=15)
                if r.status_code == 200:
                    n = import_csv(r.text)
                    s = get_summary()
                    say(f":white_check_mark: *{n} transactions imported from CSV*\n"
                        f"Total: {s.get('count',0)} transactions | "
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
