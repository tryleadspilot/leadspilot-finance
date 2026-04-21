"""
LeadsPilot Financial Intelligence Bot — FIXED
===============================================
Wise data sources:
  - /v1/transfers → bank transfers (PKR salaries, AUD wires) — confirmed works
  - CSV import → card transactions (Sendivo, Signal House, etc.) — exact names
  - NO balance statements (causes wrong names + incoming = expenses bug)
Fanbasis:
  - /checkout-sessions/transactions → revenue
Learning system:
  - Pre-loaded 41 known recipients
  - Asks about NEW unknowns only (never past)
  - Natural language replies to identify recipients
Reports:
  - Daily 7pm EST, Weekly Sunday 10am EST
  - Analytics: margin change, personal vs business alerts
"""

import os, re, csv, io, json, logging, requests, threading, time, ssl, urllib.parse
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
WISE_KEY        = os.environ["WISE_API_KEY"]
FB_KEY          = os.environ["FANBASIS_API_KEY"]
AI_KEY          = os.environ["ANTHROPIC_API_KEY"]
DATABASE_URL    = os.environ.get("DATABASE_URL", "")
CHANNEL_ID      = os.environ.get("CHANNEL_ID", "C0AUJHKE5C1")
PORT            = int(os.environ.get("PORT", 8080))
EST             = ZoneInfo("America/New_York")
WISE_BASE       = "https://api.wise.com"
FB_BASE         = "https://www.fanbasis.com/public-api"

# ── PRE-LOADED RECIPIENTS ─────────────────────────────────────────────────────
KNOWN = [
    ("Signal House SMS",             "Signal House SMS",     "SMS Cost",       "Primary SMS provider"),
    ("Sendivo",                      "Sendivo",              "SMS Cost",       "SMS provider"),
    ("Fanbasis.com",                 "Fanbasis (Jacob)",     "Data Provider",  "Jacob - data provider"),
    ("Fanbasis",                     "Fanbasis (Jacob)",     "Data Provider",  "Jacob - data provider"),
    ("ZEESHAN SHABBIR",              "Zeeshan Shabbir",      "Data Provider",  "Data provider = Ghulam Shabir"),
    ("Ghulam Shabir",                "Zeeshan Shabbir",      "Data Provider",  "Data provider = ZEESHAN SHABBIR"),
    ("Ghulam Shabir ",               "Zeeshan Shabbir",      "Data Provider",  "Data provider"),
    ("Muhammad Hisham",              "Muhammad Hisham",      "Personal",       "Suleman personal"),
    ("Muhammad Hisham ",             "Muhammad Hisham",      "Personal",       "Suleman personal"),
    ("Abdul Rehman",                 "Abdul Rehman",         "Personal",       "Suleman personal"),
    ("Starla Shanaine Pamolarco Gadiano","Starla",           "Salary",         "Filipino VA"),
    ("Niña Selvendy Amor Cayongcong","Nina",                 "Salary",         "Filipino VA"),
    ("John Isaac Cane",              "John",                 "Salary",         "Filipino VA"),
    ("Queenzen Alvarado Bensi",      "Queenzen",             "Salary",         "Filipino VA"),
    ("Annalyn Budejas",              "Annalyn",              "Salary",         "Filipino VA"),
    ("INTERACTIVE BROKERS LLC",      "Interactive Brokers",  "Investment",     "Stock investments"),
    ("Usman Ahmed",                  "Usman Ahmed",          "Rent",           "House rent + security"),
    ("Wahaj Khan",                   "Wahaj Khan",           "Loan/Personal",  "Cousin - loan + pass-through"),
    ("shayan amir khan",             "Wahaj Khan",           "Loan/Personal",  "Same as Wahaj Khan"),
    ("MOEEZ MAZHAR",                 "Moeez Mazhar",         "Hardware",       "iPhone 17 Pro"),
    ("Abdul Rehman Tahir",           "Abdul Rehman Tahir",   "Hardware",       "iPhone + MacBook"),
    ("Anthropic",                    "Anthropic",            "Software",       "AI API"),
    ("Claude",                       "Claude",               "Software",       "Claude AI"),
    ("HighLevel",                    "HighLevel",            "Software",       "CRM"),
    ("Calendly",                     "Calendly",             "Software",       "Scheduling"),
    ("Opus Virtual Offices",         "Opus Virtual Offices", "Software",       "$99/mo virtual office"),
    ("Zoom",                         "Zoom",                 "Software",       "Video calls"),
    ("N8n Cloud1",                   "N8n Cloud",            "Software",       "Automation"),
    ("Slack",                        "Slack",                "Software",       "Team chat"),
    ("Framer",                       "Framer",               "Software",       "Website builder"),
    ("Google",                       "Google",               "Software",       "Google Workspace"),
    ("Retell AI",                    "Retell AI",            "Software",       "AI voice"),
    ("Instantly",                    "Instantly",            "Software",       "Email outreach"),
    ("Whop Charan Invests",          "Whop (course)",        "Software",       "Course purchase"),
    ("Whop Rinip Ventures Ll",       "Whop Rinip Ventures",  "Business Other", "Outreach system"),
    ("Grasshopper Group Llc",        "Grasshopper Group",    "Software",       "Phone number"),
    ("Saurabh Kumar",                "Saurabh Kumar",        "Business Other", "Website build"),
    ("Abdullah Habib (Minor)",       "Abdullah Habib",       "Business Other", "Customer gen"),
    ("Inyxel Studios LLC",           "Inyxel Studios",       "Business Other", "Business expense"),
    ("Onlinejobs.ph",                "OnlineJobs.ph",        "Software",       "VA job board"),
    ("Sp Thatonestreet",             "Sp Thatonestreet",     "Business Other", "Business expense"),
    ("Ow Mulebuy.com",               "Ow Mulebuy",           "Software",       "Software expense"),
    ("TransferWise",                 "TransferWise",         "Software",       "Wise setup fee"),
    ("Bizee",                        "Bizee",                "Business Other", "Business expense"),
    ("Divisible Inc",                "Divisible Inc",        "Incoming",       "Revenue coming in - skip"),
    ("LEADS PILOT LLC",              "LeadsPilot",           "Incoming",       "Own company - skip"),
]

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
    except ImportError:
        log.error("pg8000 not installed"); return None
    except Exception as e:
        log.error(f"DB: {e}"); _db = None; return None

def init_db():
    db = get_db()
    if not db: return
    db.cursor().execute("""
        DROP TABLE IF EXISTS wise_tx;
        CREATE TABLE wise_tx (
            id         TEXT PRIMARY KEY,
            date       DATE,
            amount_aud NUMERIC(14,4),
            raw_name   TEXT,
            clean_name TEXT,
            category   TEXT,
            tx_type    TEXT,
            synced_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS fb_tx (
            id        TEXT PRIMARY KEY,
            date      DATE,
            customer  TEXT,
            email     TEXT,
            product   TEXT,
            amount    NUMERIC(14,2),
            fee       NUMERIC(14,2),
            net       NUMERIC(14,2),
            synced_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS recipients (
            raw_name   TEXT PRIMARY KEY,
            clean_name TEXT,
            category   TEXT,
            note       TEXT
        );
        CREATE TABLE IF NOT EXISTS asked_recipients (
            raw_name TEXT PRIMARY KEY,
            asked_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
        CREATE INDEX IF NOT EXISTS idx_wise_date ON wise_tx(date);
        CREATE INDEX IF NOT EXISTS idx_wise_cat  ON wise_tx(category);
        CREATE INDEX IF NOT EXISTS idx_fb_date   ON fb_tx(date);
    """)
    seed_recipients()
    log.info("DB ready")

def seed_recipients():
    db = get_db()
    if not db: return
    for raw, clean, cat, note in KNOWN:
        try:
            db.cursor().execute(
                "INSERT INTO recipients(raw_name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
                "ON CONFLICT(raw_name) DO NOTHING", (raw, clean, cat, note))
        except: pass
    log.info(f"Seeded {len(KNOWN)} recipients")

def get_recipient(raw_name):
    if not raw_name: return None
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name,category,note FROM recipients WHERE raw_name=%s", (raw_name,))
        return cur.fetchone()
    except: return None

def save_recipient(raw_name, clean_name, category, note=""):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO recipients(raw_name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(raw_name) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, note=EXCLUDED.note",
            (raw_name, clean_name, category, note))
        db.cursor().execute(
            "UPDATE wise_tx SET clean_name=%s, category=%s WHERE raw_name=%s",
            (clean_name, category, raw_name))
        db.cursor().execute(
            "INSERT INTO asked_recipients(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw_name,))
    except Exception as e:
        log.error(f"save_recipient: {e}")

def already_asked(raw_name):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM asked_recipients WHERE raw_name=%s", (raw_name,))
        return cur.fetchone() is not None
    except: return False

def mark_asked(raw_name):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO asked_recipients(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw_name,))
    except: pass

def upsert_wise(tx_id, date, amount_aud, raw_name, clean_name, category, tx_type):
    # Skip incoming transactions
    if category == "Incoming": return
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO wise_tx(id,date,amount_aud,raw_name,clean_name,category,tx_type) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, amount_aud=EXCLUDED.amount_aud",
            (tx_id, date, float(amount_aud), raw_name, clean_name, category, tx_type))
    except Exception as e:
        log.error(f"upsert_wise: {e}")

def upsert_fb(tx_id, date, customer, email, product, amount, fee, net):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO fb_tx(id,date,customer,email,product,amount,fee,net) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET amount=EXCLUDED.amount",
            (tx_id, date, customer, email, product, float(amount), float(fee), float(net)))
    except Exception as e:
        log.error(f"upsert_fb: {e}")

def kv_get(k, d=None):
    db = get_db()
    if not db: return d
    try:
        cur = db.cursor()
        cur.execute("SELECT value FROM kv WHERE key=%s", (k,))
        r = cur.fetchone(); return r[0] if r else d
    except: return d

def kv_set(k, v):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO kv(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (k, str(v)))
    except: pass

# ── DB QUERIES ────────────────────────────────────────────────────────────────
def wise_stats():
    db = get_db()
    if not db: return 0, None, None, 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), SUM(amount_aud) FROM wise_tx")
        return cur.fetchone()
    except: return 0, None, None, 0

def fb_stats():
    db = get_db()
    if not db: return 0, None, None, 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), SUM(net) FROM fb_tx")
        return cur.fetchone()
    except: return 0, None, None, 0

def spending_by_category(start=None, end=None):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = "SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*) FROM wise_tx WHERE category != 'Incoming'"
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += " GROUP BY COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def spending_by_recipient(start=None, end=None, limit=30):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = ("SELECT COALESCE(clean_name,raw_name), SUM(amount_aud), COUNT(*), "
             "COALESCE(category,'Unknown') FROM wise_tx WHERE category != 'Incoming'")
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += f" GROUP BY COALESCE(clean_name,raw_name), COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC LIMIT {limit}"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def revenue_by_customer(start=None, end=None):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = ("SELECT customer, COUNT(*) purchases, SUM(net) net, MIN(date) first, MAX(date) last "
             "FROM fb_tx WHERE 1=1")
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += " GROUP BY customer ORDER BY SUM(net) DESC LIMIT 25"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def recent_wise(n=30):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT date, amount_aud, COALESCE(clean_name,raw_name), "
            "COALESCE(category,'Unknown'), tx_type "
            "FROM wise_tx WHERE category != 'Incoming' "
            "ORDER BY date DESC, synced_at DESC LIMIT %s", (n,))
        return cur.fetchall()
    except: return []

def period_summary(start, end):
    db = get_db()
    if not db: return {}
    try:
        cur = db.cursor()
        # Fanbasis revenue (stored in AUD equivalent via Divisible Inc)
        cur.execute("SELECT SUM(net), COUNT(*) FROM fb_tx WHERE date>=%s AND date<=%s", (start, end))
        row = cur.fetchone()
        revenue = float(row[0] or 0); leads = int(row[1] or 0)
        # Business expenses in AUD
        cur.execute("""
            SELECT SUM(amount_aud) FROM wise_tx WHERE date>=%s AND date<=%s
            AND category NOT IN ('Personal','Investment','Loan/Personal','Hardware','Incoming')
            AND category IS NOT NULL
        """, (start, end))
        biz = float((cur.fetchone() or [0])[0] or 0)
        # Personal
        cur.execute("SELECT SUM(amount_aud) FROM wise_tx WHERE date>=%s AND date<=%s AND category='Personal'", (start, end))
        personal = float((cur.fetchone() or [0])[0] or 0)
        profit = revenue - biz
        margin = (profit / revenue * 100) if revenue > 0 else 0
        return {"revenue": revenue, "leads": leads, "biz_exp": biz,
                "personal": personal, "profit": profit, "margin": margin}
    except: return {}

# ── WISE SYNC ─────────────────────────────────────────────────────────────────
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

def resolve_name(account_id):
    """Get account holder name from Wise API, cached in recipients table."""
    if not account_id: return None
    cache_key = f"acct_{account_id}"
    known = get_recipient(cache_key)
    if known: return known[0]
    try:
        r = requests.get(f"{WISE_BASE}/v1/accounts/{account_id}",
            headers=wise_h(), timeout=6)
        if r.status_code == 200:
            d = r.json()
            name = (d.get("accountHolderName") or d.get("name") or
                    (d.get("details") or {}).get("accountHolderName"))
            if name:
                db = get_db()
                if db:
                    try:
                        db.cursor().execute(
                            "INSERT INTO recipients(raw_name,clean_name,category,note) "
                            "VALUES(%s,%s,%s,%s) ON CONFLICT(raw_name) DO NOTHING",
                            (cache_key, name, None, "auto-resolved"))
                    except: pass
                return name
    except: pass
    return None

def sync_wise_transfers():
    """Sync bank transfers via /v1/transfers — confirmed works for this account."""
    pid = get_pid(); offset = 0; new_unknowns = []
    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(), params={"profile":pid,"limit":100,"offset":offset}, timeout=20)
            r.raise_for_status()
            data  = r.json()
            batch = data if isinstance(data, list) else data.get("content",[])
            if not batch: break
            for tx in batch:
                if tx.get("status") not in ("outgoing_payment_sent",): continue
                # Use sourceValue (AUD amount that left account)
                amount_aud = float(tx.get("sourceValue") or 0)
                if amount_aud == 0: continue
                acct_id  = tx.get("targetAccount")
                raw_name = resolve_name(acct_id) if acct_id else None
                det = tx.get("details") or {}
                if not raw_name:
                    raw_name = det.get("reference") or tx.get("reference") or ""
                if not raw_name: raw_name = f"transfer-{tx.get('id','')}"
                date_s = tx.get("created") or tx.get("createdAt","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00")).date()
                except: d = datetime.now().date()
                tx_id = str(tx.get("id",""))
                known = get_recipient(raw_name)
                clean = known[0] if known else raw_name
                cat   = known[1] if known else None
                if not known and not already_asked(raw_name):
                    new_unknowns.append((raw_name, amount_aud, d))
                upsert_wise(tx_id, d, amount_aud, raw_name, clean, cat, "TRANSFER")
            if len(batch) < 100: break
            offset += 100
        except Exception as e:
            log.error(f"transfers: {e}"); break
    return new_unknowns

def import_csv(text):
    """
    Import Wise CSV export for card transactions.
    CSV has exact merchant names (Signal House SMS, Sendivo etc).
    Only import COMPLETED OUT transactions.
    """
    reader = csv.DictReader(io.StringIO(text))
    new_unknowns = []; count = 0
    for row in reader:
        if row.get("Direction","").strip() != "OUT": continue
        if row.get("Status","").strip() != "COMPLETED": continue
        if not row.get("ID","").strip().startswith("CARD_TRANSACTION"): continue
        tx_id    = row.get("ID","").strip()
        raw_name = row.get("Target name","").strip() or "Unknown"
        amount   = float(row.get("Source amount (after fees)","0") or 0)
        date_s   = row.get("Created on","").strip()[:10]
        try: d = datetime.strptime(date_s, "%Y-%m-%d").date()
        except: d = datetime.now().date()
        known = get_recipient(raw_name)
        clean = known[0] if known else raw_name
        cat   = known[1] if known else None
        if not known and not already_asked(raw_name):
            new_unknowns.append((raw_name, amount, d))
        upsert_wise(tx_id, d, amount, raw_name, clean, cat, "CARD")
        count += 1
    log.info(f"CSV import: {count} card transactions")
    return new_unknowns

def sync_fanbasis():
    page = 1; total = 0
    while True:
        try:
            r = requests.get(f"{FB_BASE}/checkout-sessions/transactions",
                headers={"x-api-key":FB_KEY,"Content-Type":"application/json"},
                params={"page":page,"per_page":100}, timeout=20)
            log.info(f"Fanbasis page {page}: HTTP {r.status_code}")
            if r.status_code != 200:
                log.error(f"Fanbasis error: {r.text[:200]}"); break
            data = r.json().get("data",{})
            txs  = data.get("transactions",[]) if isinstance(data,dict) else []
            if not txs: break
            for tx in txs:
                tx_id    = str(tx.get("id",""))
                fan      = tx.get("fan",{}) or {}
                service  = tx.get("service",{}) or {}
                customer = fan.get("name","Unknown")
                email    = fan.get("email","")
                product  = service.get("title","Unknown")
                amount   = float(service.get("price",0))
                fee      = float(tx.get("fee_amount",0))
                net      = float(tx.get("net_amount", amount-fee))
                date_s   = tx.get("transaction_date") or tx.get("created_at","")
                try: d = datetime.fromisoformat(str(date_s).replace("Z","+00:00")).date()
                except: d = datetime.now().date()
                if tx_id: upsert_fb(tx_id, d, customer, email, product, amount, fee, net); total += 1
            if not data.get("pagination",{}).get("has_more",False): break
            page += 1
        except Exception as e:
            log.error(f"fanbasis sync: {e}"); break
    log.info(f"Fanbasis: {total} transactions")
    return total

# ── ASK ABOUT UNKNOWNS ────────────────────────────────────────────────────────
def ask_new_unknowns(unknowns, client):
    seen = {}
    for raw_name, aud, d in unknowns:
        if raw_name not in seen: seen[raw_name] = (aud, d)

    to_ask = [(n, u, d) for n,(u,d) in seen.items()
              if not get_recipient(n) and not already_asked(n)]
    if not to_ask: return

    for raw_name,_,_ in to_ask: mark_asked(raw_name)

    if len(to_ask) == 1:
        raw_name, aud, d = to_ask[0]
        msg = (f":question: *New transaction — who is this?*\n"
               f"*`{raw_name}`* — {aud:,.2f} AUD on {d}\n\n"
               f"Just tell me naturally:\n"
               f"• _{raw_name} is our SMS provider_\n"
               f"• _{raw_name} is a personal expense_")
    else:
        msg = f":question: *{len(to_ask)} new recipients I haven't seen before:*\n\n"
        for raw_name, aud, d in to_ask[:15]:
            msg += f"• *`{raw_name}`* — {aud:,.2f} AUD ({d})\n"
        msg += "\nJust tell me who each one is naturally."

    try:
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"ask_unknowns: {e}")

# ── REPORTS ───────────────────────────────────────────────────────────────────
def daily_report(client):
    now   = datetime.now(EST)
    today = now.date()
    s     = period_summary(today, today)
    bals  = get_balances()
    by_cat = spending_by_category(today, today)
    msg  = f":sun_with_face: *Daily Report — {now.strftime('%B %d, %Y')}*\n\n"
    msg += f"*Revenue:* {s.get('revenue',0):,.2f} AUD ({s.get('leads',0)} leads)\n"
    msg += f"*Business spend:* {s.get('biz_exp',0):,.2f} AUD\n"
    msg += f"*Profit:* {s.get('profit',0):,.2f} AUD | Margin: {s.get('margin',0):.1f}%\n"
    msg += f"*Personal spend:* {s.get('personal',0):,.2f} AUD\n"
    if by_cat:
        msg += "\n*Spend breakdown:*\n"
        for cat, total, cnt in by_cat:
            msg += f"  - {cat}: {float(total):,.2f} AUD\n"
    msg += "\n*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"daily_report: {e}")

def weekly_report(client):
    now        = datetime.now(EST)
    week_end   = now.date()
    week_start = week_end - timedelta(days=6)
    prev_end   = week_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=6)
    curr = period_summary(week_start, week_end)
    prev = period_summary(prev_start, prev_end)
    by_cat   = spending_by_category(week_start, week_end)
    top_cust = revenue_by_customer(week_start, week_end)
    margin_change  = curr.get("margin",0)  - prev.get("margin",0)
    rev_change     = curr.get("revenue",0) - prev.get("revenue",0)
    personal_change= curr.get("personal",0)- prev.get("personal",0)
    msg  = f":bar_chart: *Weekly Report — {week_start} to {week_end}*\n\n"
    msg += f"*Revenue:* {curr.get('revenue',0):,.2f} AUD ({curr.get('leads',0)} leads)"
    if rev_change != 0:
        msg += f" {'↑' if rev_change>0 else '↓'} {abs(rev_change):,.0f} AUD vs last week"
    msg += f"\n*Business spend:* {curr.get('biz_exp',0):,.2f} AUD\n"
    msg += f"*Profit:* {curr.get('profit',0):,.2f} AUD\n"
    msg += f"*Margin:* {curr.get('margin',0):.1f}%"
    if margin_change != 0:
        msg += f" {'↑' if margin_change>0 else '↓'} {abs(margin_change):.1f}% vs last week"
    if margin_change < -5:
        msg += f"\n:warning: *Margin dropped {abs(margin_change):.1f}% — review spending*"
    if personal_change > 100:
        msg += f"\n:eyes: *Personal expenses up {personal_change:,.0f} AUD vs last week*"
    if by_cat:
        msg += "\n\n*Spending by category:*\n"
        for cat, total, cnt in by_cat:
            msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
    if top_cust:
        msg += "\n*Top clients this week:*\n"
        for cust, cnt, net, first, last in top_cust[:5]:
            msg += f"  - {cust}: {float(net):,.2f} AUD ({cnt} charges)\n"
    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"weekly_report: {e}")

# ── AI ────────────────────────────────────────────────────────────────────────
def is_recipient_info(text):
    low = text.lower()
    triggers = ["is our","is a ","is my ","is the ","salary","provider",
                "software","expense","personal","rent","loan","he is","she is","they are"]
    return any(t in low for t in triggers) and len(text.split()) < 25

def handle_recipient_info(text):
    system = """Extract recipient info from this message. Return JSON only:
{"raw_name": "...", "clean_name": "...", "category": "...", "note": "..."}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Loan, Hardware, Business Other
If unclear return: {"error": "unclear"}"""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":200,"system":system,
                  "messages":[{"role":"user","content":text}]},timeout=15)
        if r.status_code == 200:
            raw = r.json()["content"][0]["text"].strip()
            data = json.loads(re.sub(r'```\w*|```','',raw).strip())
            if "error" not in data and data.get("raw_name"):
                save_recipient(data["raw_name"],data["clean_name"],data["category"],data.get("note",""))
                return (f":white_check_mark: Got it! *{data['raw_name']}* = "
                        f"*{data['clean_name']}* ({data['category']}). All transactions updated.")
    except: pass
    return None

def answer(q, client=None):
    log.info(f"Q: {q}")
    if client and is_recipient_info(q):
        r = handle_recipient_info(q)
        if r: return r

    try: bals = get_balances()
    except: bals = []

    w_count, w_oldest, w_newest, w_total = wise_stats()
    f_count, f_oldest, f_newest, f_net   = fb_stats()
    now        = datetime.now(EST)
    today      = now.date()
    month_start= today.replace(day=1)
    week_start = today - timedelta(days=today.weekday())
    month = period_summary(month_start, today)
    week  = period_summary(week_start, today)
    by_cat   = spending_by_category()
    by_recip = spending_by_recipient(limit=30)
    by_cust  = revenue_by_customer()
    recent   = recent_wise(40)

    bal_text = "\n".join(f"  {b}" for b in bals) or "  unavailable"
    cat_text = "\n".join(
        f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]} payments)" for r in by_cat) or "  none"
    recip_text = "\n".join(
        f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]} payments)" for r in by_recip) or "  none"
    cust_text = "\n".join(
        f"  {r[0]}: {r[1]} charges | LTV {float(r[2]):,.2f} AUD | {r[3]} to {r[4]}"
        for r in by_cust) or "  none"
    recent_text = "\n".join(
        f"  {r[0]}: {float(r[1]):,.2f} AUD -> {r[2]} [{r[3]}]"
        for r in recent) or "  none"

    system = f"""You are the financial intelligence bot for LeadsPilot — Suleman's SMS lead gen business.
Answer naturally and directly. All amounts in AUD. Calculate anything asked.

TODAY: {today} | Month: {month_start} to {today} | Week: {week_start} to {today}

WISE BALANCES (live):
{bal_text}

WISE DB: {w_count} transactions ({w_oldest} to {w_newest}) | Total spent: {float(w_total or 0):,.2f} AUD
FANBASIS DB: {f_count} transactions ({f_oldest} to {f_newest}) | Total net revenue: {float(f_net or 0):,.2f} AUD

THIS MONTH:
  Revenue: {month.get('revenue',0):,.2f} AUD ({month.get('leads',0)} leads)
  Business expenses: {month.get('biz_exp',0):,.2f} AUD
  Personal: {month.get('personal',0):,.2f} AUD
  Profit: {month.get('profit',0):,.2f} AUD | Margin: {month.get('margin',0):.1f}%

THIS WEEK:
  Revenue: {week.get('revenue',0):,.2f} AUD ({week.get('leads',0)} leads)
  Business expenses: {week.get('biz_exp',0):,.2f} AUD
  Profit: {week.get('profit',0):,.2f} AUD | Margin: {week.get('margin',0):.1f}%

ALL-TIME SPENDING BY CATEGORY:
{cat_text}

ALL-TIME SPENDING BY RECIPIENT:
{recip_text}

FANBASIS CLIENTS — LIFETIME VALUE:
{cust_text}

RECENT WISE TRANSACTIONS:
{recent_text}

Business categories: SMS Cost, Data Provider, Salary, Software, Business Other, Rent
Personal categories: Personal, Hardware, Loan/Personal
Investment: Investment (Interactive Brokers — asset not expense)
Profit = Fanbasis revenue minus business expenses only."""

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":600,"system":system,
                  "messages":[{"role":"user","content":q}]},timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e:
        log.error(f"Claude: {e}")
    return "Could not answer right now."

# ── STARTUP ───────────────────────────────────────────────────────────────────
def full_sync(client, csv_text=None):
    unknowns = []
    # Sync bank transfers
    unknowns += sync_wise_transfers()
    # Import CSV for card transactions if provided
    if csv_text:
        unknowns += import_csv(csv_text)
    else:
        # Try to load CSV from same directory
        for path in ["transaction-history.csv", "/app/transaction-history.csv"]:
            if os.path.exists(path):
                with open(path, "r") as f:
                    unknowns += import_csv(f.read())
                break
    # Sync Fanbasis
    sync_fanbasis()
    return unknowns

def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_LeadsPilot Finance Bot loading — syncing all data..._")
        unknowns = full_sync(client)
        w_count, w_oldest, w_newest, w_total = wise_stats()
        f_count, f_oldest, f_newest, f_net   = fb_stats()
        bals   = get_balances()
        by_cat = spending_by_category()
        month  = period_summary(datetime.now(EST).date().replace(day=1), datetime.now(EST).date())
        # Latest transaction
        recent = recent_wise(1)
        latest = ""
        if recent:
            r = recent[0]
            latest = f"\n*Latest transaction:* {r[0]} — {float(r[1]):,.2f} AUD to *{r[2]}*"
        msg  = f"*LeadsPilot Finance Bot online* :white_check_mark:\n\n"
        msg += f"*Wise:* {w_count} transactions ({w_oldest} to {w_newest})\n"
        msg += f"*Fanbasis:* {f_count} transactions ({f_oldest} to {f_newest})\n"
        msg += f"*All-time revenue:* {float(f_net or 0):,.2f} AUD\n"
        msg += f"*Total Wise spend:* {float(w_total or 0):,.2f} AUD\n"
        msg += f"*This month profit:* {month.get('profit',0):,.2f} AUD | Margin: {month.get('margin',0):.1f}%\n"
        msg += "\n*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if by_cat:
            msg += "\n\n*All-time spending by category:*\n"
            for cat, total, cnt in by_cat:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        msg += latest
        msg += "\n\nAsk me anything — profit, margin, LTV, spending, revenue."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
        if unknowns:
            time.sleep(2)
            ask_new_unknowns(unknowns, client)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(180)
        try:
            unknowns = sync_wise_transfers()
            sync_fanbasis()
            if unknowns: ask_new_unknowns(unknowns, client)
        except Exception as e:
            log.error(f"worker: {e}")

def scheduler(client):
    last_daily = None; last_weekly = None
    while True:
        time.sleep(60)
        try:
            now = datetime.now(EST); today = now.date()
            if now.hour == 19 and now.minute < 2 and last_daily != today:
                daily_report(client); last_daily = today
            if now.weekday() == 6 and now.hour == 10 and now.minute < 2 and last_weekly != today:
                weekly_report(client); last_weekly = today
        except Exception as e: log.error(f"scheduler: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status":"LeadsPilot Finance Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    wc,_,_,wt = wise_stats(); fc,_,_,fn = fb_stats()
    return jsonify({"status":"ok","wise_tx":wc,"fb_tx":fc,"net_revenue":float(fn or 0)})

@flask_app.route("/webhook/fanbasis", methods=["POST"])
def fb_webhook():
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type","unknown")
        log.info(f"Fanbasis webhook: {event_type}")
        if "dispute" in event_type:
            buyer = payload.get("buyer",{}) or {}
            item  = payload.get("item",{}) or {}
            amt   = float(payload.get("amount",0))/100
            try:
                slack_app.client.chat_postMessage(channel=CHANNEL_ID,
                    text=f":rotating_light: *Dispute Filed!*\n"
                         f"Customer: *{buyer.get('name','Unknown')}*\n"
                         f"Product: *{item.get('name','Unknown')}*\n"
                         f"Amount: *{amt:,.2f} AUD*\nCheck Fanbasis dashboard.")
            except: pass
        elif event_type in ("payment.succeeded","product.purchased","subscription.renewed"):
            buyer  = payload.get("buyer",{}) or {}
            item   = payload.get("item",{}) or {}
            tx_id  = str(payload.get("payment_id") or payload.get("id",""))
            amount = float(payload.get("amount",0))/100
            upsert_fb(tx_id, datetime.now().date(), buyer.get("name","Unknown"),
                      buyer.get("email",""), item.get("name","Unknown"), amount, 0, amount)
        return jsonify({"status":"ok"}), 200
    except Exception as e:
        log.error(f"fb webhook: {e}"); return jsonify({"status":"error"}), 500

@flask_app.route("/webhook/wise", methods=["POST","GET"])
def wise_webhook():
    if request.method == "GET":
        return jsonify({"status":"ok"}), 200
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type","unknown")
        log.info(f"Wise webhook: {event_type}")
        if event_type in ("balances#update","transfers#state-change"):
            threading.Thread(target=lambda: (
                sync_wise_transfers(),
                ask_new_unknowns(sync_wise_transfers(), slack_app.client)
            ), daemon=True).start()
        return jsonify({"status":"ok"}), 200
    except Exception as e:
        log.error(f"wise webhook: {e}"); return jsonify({"status":"error"}), 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>","",text or "").strip()
    if not text: return
    # Handle CSV file content pasted directly
    if "ID,Status,Direction" in text[:50]:
        unknowns = import_csv(text)
        if unknowns: ask_new_unknowns(unknowns, slack_app.client)
        wc,_,_,wt = wise_stats()
        say(f":white_check_mark: CSV imported. DB now has {wc} transactions, {float(wt or 0):,.2f} AUD total spend.")
        return
    say("_Checking..._")
    response = answer(text, slack_app.client)
    if response: say(response)

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text",""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
    # Handle CSV file uploads
    for f in event.get("files",[]):
        if f.get("name","").endswith(".csv"):
            url = f.get("url_private") or f.get("url_private_download")
            if url:
                r = requests.get(url, headers={"Authorization":f"Bearer {SLACK_BOT_TOKEN}"}, timeout=15)
                if r.status_code == 200:
                    unknowns = import_csv(r.text)
                    if unknowns: ask_new_unknowns(unknowns, slack_app.client)
                    wc,_,_,wt = wise_stats()
                    say(f":white_check_mark: CSV imported. {wc} transactions, {float(wt or 0):,.2f} AUD total.")
                return
    t = (event.get("text") or "").strip()
    if t: process(t, say)

if __name__ == "__main__":
    log.info("Starting LeadsPilot Finance Bot...")
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    threading.Timer(5, startup, args=[slack_app.client]).start()
    threading.Thread(target=worker, args=[slack_app.client], daemon=True).start()
    threading.Thread(target=scheduler, args=[slack_app.client], daemon=True).start()
    handler.start()
