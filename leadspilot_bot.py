"""
LeadsPilot Financial Intelligence Bot — FINAL
==============================================
- Wise: bank transfers + card transactions (via balance statements)
- Fanbasis: revenue tracking
- Smart learning: asks about NEW unknown recipients only, never past
- Pre-loaded knowledge of all known recipients from conversation
- Daily report: 7pm EST
- Weekly report: Sunday 10am EST
- Analytics: margin tracking, personal vs business, alerts
- No commands — pure natural language via Claude
- Disputes only from Fanbasis (no payment success/fail noise)
"""

import os, re, json, logging, requests, threading, time, ssl, urllib.parse
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

# ── PRE-LOADED KNOWLEDGE ──────────────────────────────────────────────────────
# Everything Suleman told us — loaded into DB on first start
KNOWN_RECIPIENTS = [
    # raw_name, clean_name, category, note
    ("Signal House SMS",            "Signal House SMS",      "SMS Cost",        "Primary SMS provider"),
    ("Sendivo",                     "Sendivo",               "SMS Cost",        "SMS provider"),
    ("Fanbasis.com",                "Fanbasis (Jacob)",      "Data Provider",   "Jacob data provider"),
    ("Fanbasis",                    "Fanbasis (Jacob)",      "Data Provider",   "Jacob data provider"),
    ("ZEESHAN SHABBIR",             "Zeeshan Shabbir",       "Data Provider",   "Data provider, same as Ghulam Shabir"),
    ("Ghulam Shabir",               "Zeeshan Shabbir",       "Data Provider",   "Data provider, same as Zeeshan Shabbir"),
    ("Muhammad Hisham",             "Muhammad Hisham",       "Personal",        "Suleman personal expense"),
    ("Muhammad Hisham ",            "Muhammad Hisham",       "Personal",        "Suleman personal expense"),
    ("Abdul Rehman",                "Abdul Rehman",          "Personal",        "Suleman personal expense"),
    ("Starla Shanaine Pamolarco Gadiano", "Starla",         "Salary",          "Filipino staff"),
    ("Niña Selvendy Amor Cayongcong","Nina",                 "Salary",          "Filipino staff"),
    ("John Isaac Cane",             "John",                  "Salary",          "Filipino staff"),
    ("Queenzen Alvarado Bensi",     "Queenzen",              "Salary",          "Filipino staff"),
    ("Annalyn Budejas",             "Annalyn",               "Salary",          "Filipino staff"),
    ("INTERACTIVE BROKERS LLC",     "Interactive Brokers",   "Investment",      "Stock investments"),
    ("Usman Ahmed",                 "Usman Ahmed",           "Rent",            "House rent + security"),
    ("Wahaj Khan",                  "Wahaj Khan",            "Loan/Personal",   "Cousin - loan + client pass-through"),
    ("shayan amir khan",            "Wahaj Khan",            "Loan/Personal",   "Same as Wahaj Khan, cousin"),
    ("MOEEZ MAZHAR",                "Moeez Mazhar",          "Hardware",        "Bought iPhone 17 Pro from him"),
    ("Abdul Rehman Tahir",          "Abdul Rehman Tahir",    "Hardware",        "iPhone 17 Pro Max + MacBook"),
    ("Anthropic",                   "Anthropic",             "Software",        "AI API costs"),
    ("Claude",                      "Claude",                "Software",        "Claude AI subscription"),
    ("HighLevel",                   "HighLevel",             "Software",        "CRM software"),
    ("Calendly",                    "Calendly",              "Software",        "Scheduling software"),
    ("Opus Virtual Offices",        "Opus Virtual Offices",  "Software",        "Virtual office - $99/mo"),
    ("Zoom",                        "Zoom",                  "Software",        "Video calls"),
    ("N8n Cloud1",                  "N8n Cloud",             "Software",        "Automation software"),
    ("Slack",                       "Slack",                 "Software",        "Team communication"),
    ("Framer",                      "Framer",                "Software",        "Website builder"),
    ("Google",                      "Google",                "Software",        "Google Workspace"),
    ("Retell AI",                   "Retell AI",             "Software",        "AI voice software"),
    ("Instantly",                   "Instantly",             "Software",        "Email outreach"),
    ("Whop Charan Invests",         "Whop (course)",         "Software",        "Course purchase"),
    ("Whop Rinip Ventures Ll",      "Whop Rinip Ventures",   "Business Other",  "Outreach system build"),
    ("Grasshopper Group Llc",       "Grasshopper Group",     "Software",        "Phone number for Wise account"),
    ("Saurabh Kumar",               "Saurabh Kumar",         "Business Other",  "Website build"),
    ("Abdullah Habib (Minor)",      "Abdullah Habib",        "Business Other",  "Customer gen attempt - did not work"),
    ("Inyxel Studios LLC",          "Inyxel Studios",        "Business Other",  "Business expense"),
    ("Onlinejobs.ph",               "OnlineJobs.ph",         "Software",        "Job board for Filipino VAs"),
    ("Sp Thatonestreet",            "Sp Thatonestreet",      "Business Other",  "Business expense"),
    ("Ow Mulebuy.com",              "Ow Mulebuy",            "Software",        "Software expense"),
    ("TransferWise",                "TransferWise",          "Software",        "Wise account setup fee"),
    ("Jonabelle Bayona Cahigas",    "Jonabelle",             "Unknown",         "Unknown - needs identification"),
    ("Gia Breeana Gentile",         "Gia Breeana Gentile",   "Unknown",         "Unknown - needs identification"),
    ("Bizee",                       "Bizee",                 "Business Other",  "Business expense"),
]

# Software keywords - auto-categorize without asking
SOFTWARE_KEYWORDS = [
    "anthropic","claude","highlevel","calendly","zoom","n8n","slack","framer",
    "google","retell","instantly","whop","grasshopper","opus virtual","zapier",
    "notion","dropbox","github","figma","linear","loom","typeform","airtable",
    "hubspot","mailchimp","stripe","twilio","openai","midjourney"
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
            amount_raw NUMERIC(14,4),
            currency   TEXT,
            amount_usd NUMERIC(14,2),
            type       TEXT,
            raw_name   TEXT,
            clean_name TEXT,
            category   TEXT,
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
            note       TEXT,
            learned_at TIMESTAMP DEFAULT NOW()
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
    log.info("DB ready")
    seed_known_recipients()

def seed_known_recipients():
    """Load all pre-known recipients into DB so bot never asks about them."""
    db = get_db()
    if not db: return
    for raw, clean, cat, note in KNOWN_RECIPIENTS:
        try:
            db.cursor().execute(
                "INSERT INTO recipients(raw_name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
                "ON CONFLICT(raw_name) DO NOTHING",
                (raw, clean, cat, note))
        except: pass
    log.info(f"Seeded {len(KNOWN_RECIPIENTS)} known recipients")

def kv_get(k, d=None):
    db = get_db()
    if not db: return d
    try:
        cur = db.cursor(); cur.execute("SELECT value FROM kv WHERE key=%s", (k,))
        r = cur.fetchone(); return r[0] if r else d
    except: return d

def kv_set(k, v):
    db = get_db()
    if not db: return
    try: db.cursor().execute(
        "INSERT INTO kv(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
        (k, str(v)))
    except: pass

def get_recipient(raw_name):
    if not raw_name: return None
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name, category, note FROM recipients WHERE raw_name=%s", (raw_name,))
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
        # Mark as no longer needing to ask
        db.cursor().execute(
            "INSERT INTO asked_recipients(raw_name) VALUES(%s) ON CONFLICT DO NOTHING",
            (raw_name,))
    except Exception as e: log.error(f"save_recipient: {e}")

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
    try: db.cursor().execute(
        "INSERT INTO asked_recipients(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw_name,))
    except: pass

def upsert_wise(tx_id, date, raw, cur_code, usd, tx_type, raw_name, clean_name, category):
    db = get_db()
    if not db: return
    try: db.cursor().execute(
        "INSERT INTO wise_tx(id,date,amount_raw,currency,amount_usd,type,raw_name,clean_name,category) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET "
        "clean_name=EXCLUDED.clean_name, category=EXCLUDED.category, amount_usd=EXCLUDED.amount_usd",
        (tx_id, date, float(raw), cur_code, float(usd), tx_type, raw_name, clean_name, category))
    except Exception as e: log.error(f"upsert_wise: {e}")

def upsert_fb(tx_id, date, customer, email, product, amount, fee, net):
    db = get_db()
    if not db: return
    try: db.cursor().execute(
        "INSERT INTO fb_tx(id,date,customer,email,product,amount,fee,net) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET amount=EXCLUDED.amount",
        (tx_id, date, customer, email, product, float(amount), float(fee), float(net)))
    except Exception as e: log.error(f"upsert_fb: {e}")

# ── CURRENCY ──────────────────────────────────────────────────────────────────
_fx = {}
def to_usd(amount, cur):
    if not amount: return 0.0
    v = float(amount)
    if cur == "USD": return round(v, 2)
    if cur not in _fx:
        try:
            r = requests.get(f"https://api.exchangerate-api.com/v4/latest/{cur}", timeout=6)
            _fx[cur] = r.json()["rates"]["USD"]
        except:
            _fx[cur] = {"AUD":0.64,"GBP":1.27,"EUR":1.08,"CAD":0.73,"PKR":0.0036,"PHP":0.017}.get(cur,1.0)
    return round(v * _fx[cur], 2)

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
        if v > 0:
            usd = to_usd(v, c)
            out.append(f"{c}: {v:,.2f}" + (f" (~${usd:,.0f} USD)" if c!="USD" else ""))
    return out

def get_account_ids():
    pid = get_pid()
    r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
        headers=wise_h(), params={"types":"STANDARD"}, timeout=10)
    r.raise_for_status()
    return [(b.get("id"), b.get("amount",{}).get("currency","AUD"))
            for b in r.json() if b.get("id")]

def resolve_name(account_id):
    """Get real account holder name from Wise - cached in DB."""
    if not account_id: return None
    # Check DB cache
    db = get_db()
    if db:
        try:
            cur = db.cursor()
            cur.execute("SELECT clean_name FROM recipients WHERE raw_name=%s", (f"acct_{account_id}",))
            r = cur.fetchone()
            if r: return r[0]
        except: pass
    try:
        r = requests.get(f"{WISE_BASE}/v1/accounts/{account_id}",
            headers=wise_h(), timeout=6)
        if r.status_code == 200:
            d = r.json()
            name = (d.get("accountHolderName") or d.get("name") or
                    (d.get("details") or {}).get("accountHolderName"))
            if name and db:
                try: db.cursor().execute(
                    "INSERT INTO recipients(raw_name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
                    "ON CONFLICT(raw_name) DO NOTHING",
                    (f"acct_{account_id}", name, None, "auto-resolved"))
                except: pass
            return name
    except: pass
    return None

def sync_wise_all():
    """
    Sync ALL Wise transactions:
    - Bank transfers via /v1/transfers (all time, no limit)
    - Card transactions via balance statements in 90-day chunks
    Returns list of NEW unknown recipients to ask about.
    """
    new_unknowns = []  # (raw_name, amount_usd, date)

    # ─ Bank transfers ─
    pid = get_pid(); offset = 0
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
                # Use sourceValue (what left your account) and sourceCurrency
                raw  = float(tx.get("sourceValue") or 0)
                cur  = tx.get("sourceCurrency","AUD")
                # If target is USD use targetValue directly for accuracy
                if tx.get("targetCurrency") == "USD":
                    usd = float(tx.get("targetValue") or to_usd(raw, cur))
                else:
                    usd  = to_usd(raw, cur)
                det  = tx.get("details") or {}
                # Try to get real name from targetAccount
                acct_id  = tx.get("targetAccount")
                raw_name = resolve_name(acct_id) if acct_id else None
                if not raw_name:
                    raw_name = det.get("reference") or tx.get("reference") or f"transfer-{tx.get('id','')}"
                date_s = tx.get("created") or tx.get("createdAt","")
                try: d = datetime.fromisoformat(date_s.replace("Z","+00:00")).date()
                except: d = datetime.now().date()
                tx_id = str(tx.get("id",""))
                known = get_recipient(raw_name)
                clean = known[0] if known else raw_name
                cat   = known[1] if known else None
                if not known and not already_asked(raw_name):
                    new_unknowns.append((raw_name, usd, d))
                upsert_wise(tx_id, d, raw, cur, usd, "TRANSFER", raw_name, clean, cat)
            if len(batch) < 100: break
            offset += 100
        except Exception as e: log.error(f"transfers: {e}"); break

    # ─ Card transactions via balance statements ─
    now   = datetime.now(timezone.utc)
    start = datetime(2025, 11, 17, tzinfo=timezone.utc)
    for acct_id, currency in get_account_ids():
        chunk_end = now
        while chunk_end > start:
            chunk_start = max(chunk_end - timedelta(days=89), start)
            try:
                r = requests.get(
                    f"{WISE_BASE}/v1/profiles/{get_pid()}/balance-statements/{acct_id}/statement.json",
                    headers=wise_h(),
                    params={"currency":currency,
                            "intervalStart":chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "intervalEnd":chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "type":"COMPACT"},
                    timeout=20)
                if r.status_code != 200: break
                for tx in r.json().get("transactions",[]):
                    val = float(tx.get("amount",{}).get("value",0))
                    if val >= 0: continue  # Only outgoing (negative = debit)
                    amount = abs(val)
                    usd    = to_usd(amount, currency)
                    det    = tx.get("details",{}) or {}
                    # Merchant name is in details
                    raw_name = (det.get("merchant") or det.get("description") or
                                (det.get("recipient") or {}).get("name") if isinstance(det.get("recipient"),dict) else None or
                                det.get("type") or "Unknown")
                    date_s = tx.get("date") or tx.get("createdAt","")
                    try: d = datetime.fromisoformat(date_s.replace("Z","+00:00")).date()
                    except: d = datetime.now().date()
                    ext_id = str(tx.get("referenceNumber") or tx.get("id") or f"{date_s}_{amount}")
                    known  = get_recipient(raw_name)
                    clean  = known[0] if known else raw_name
                    cat    = known[1] if known else None
                    # Auto-categorize software
                    if not cat:
                        for kw in SOFTWARE_KEYWORDS:
                            if kw in raw_name.lower():
                                cat = "Software"; break
                    if not known and not already_asked(raw_name) and cat != "Software":
                        new_unknowns.append((raw_name, usd, d))
                    upsert_wise(ext_id, d, amount, currency, usd,
                                "CARD", raw_name, clean, cat or ("Software" if cat == "Software" else None))
            except Exception as e: log.error(f"statement {currency}: {e}")
            chunk_end = chunk_start

    log.info(f"Wise sync done. {len(new_unknowns)} new unknowns.")
    return new_unknowns

def sync_fanbasis():
    page = 1; total = 0
    while True:
        try:
            r = requests.get(f"{FB_BASE}/checkout-sessions/transactions",
                headers={"x-api-key":FB_KEY}, params={"page":page,"per_page":100}, timeout=20)
            if r.status_code != 200: break
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
                net      = float(tx.get("net_amount",amount-fee))
                date_s   = tx.get("transaction_date") or tx.get("created_at","")
                try: d = datetime.fromisoformat(str(date_s).replace("Z","+00:00")).date()
                except: d = datetime.now().date()
                if tx_id: upsert_fb(tx_id, d, customer, email, product, amount, fee, net); total += 1
            if not data.get("pagination",{}).get("has_more",False): break
            page += 1
        except Exception as e: log.error(f"fb sync: {e}"); break
    return total

# ── ASK ABOUT NEW UNKNOWNS ────────────────────────────────────────────────────
def ask_new_unknowns(unknowns, client):
    """
    Only ask about NEW recipients never seen before.
    Groups them into ONE message. Smart — knows software = business.
    """
    seen = {}
    for raw_name, usd, d in unknowns:
        if raw_name not in seen:
            seen[raw_name] = (usd, d)

    to_ask = [(n, u, d) for n, (u, d) in seen.items()
              if not get_recipient(n) and not already_asked(n)]

    if not to_ask: return

    for raw_name, _, _ in to_ask:
        mark_asked(raw_name)

    if len(to_ask) == 1:
        raw_name, usd, d = to_ask[0]
        msg = (f":question: *New transaction — who is this?*\n"
               f"*`{raw_name}`* — ${usd:,.0f} USD on {d}\n\n"
               f"Just reply naturally, e.g.:\n"
               f"• _\"{raw_name} is our SMS provider\"_\n"
               f"• _\"{raw_name} is a salary payment to Nina\"_\n"
               f"• _\"{raw_name} is a personal expense\"_")
    else:
        msg = f":question: *{len(to_ask)} new recipients I haven't seen before:*\n\n"
        for raw_name, usd, d in to_ask[:15]:
            msg += f"• *`{raw_name}`* — ${usd:,.0f} USD ({d})\n"
        msg += "\nJust tell me who each one is naturally — e.g. _\"Signal House is our SMS provider\"_"

    try:
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e:
        log.error(f"ask_unknowns: {e}")

# ── DB QUERIES ────────────────────────────────────────────────────────────────
def wise_stats():
    db = get_db()
    if not db: return 0, None, None, 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), SUM(amount_usd) FROM wise_tx")
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
        q = "SELECT COALESCE(category,'Unknown'), SUM(amount_usd), COUNT(*) FROM wise_tx WHERE 1=1"
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += " GROUP BY COALESCE(category,'Unknown') ORDER BY SUM(amount_usd) DESC"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def spending_by_recipient(start=None, end=None, limit=25):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = ("SELECT COALESCE(clean_name,raw_name), SUM(amount_usd), COUNT(*), "
             "COALESCE(category,'Unknown') FROM wise_tx WHERE 1=1")
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += f" GROUP BY COALESCE(clean_name,raw_name), COALESCE(category,'Unknown') ORDER BY SUM(amount_usd) DESC LIMIT {limit}"
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
            "SELECT date, amount_usd, amount_raw, currency, "
            "COALESCE(clean_name,raw_name), category, type "
            "FROM wise_tx ORDER BY date DESC, synced_at DESC LIMIT %s", (n,))
        return cur.fetchall()
    except: return []

def period_summary(start, end):
    """Get combined P&L for a period."""
    db = get_db()
    if not db: return {}
    try:
        cur = db.cursor()
        # Revenue
        cur.execute("SELECT SUM(net), COUNT(*) FROM fb_tx WHERE date>=%s AND date<=%s", (start, end))
        rev_row = cur.fetchone()
        revenue = float(rev_row[0] or 0)
        leads   = int(rev_row[1] or 0)
        # Business expenses
        cur.execute("""
            SELECT SUM(amount_usd) FROM wise_tx
            WHERE date>=%s AND date<=%s
            AND category NOT IN ('Personal','Investment','Loan/Personal','Hardware','Unknown')
            AND category IS NOT NULL
        """, (start, end))
        biz_exp = float((cur.fetchone() or [0])[0] or 0)
        # Personal
        cur.execute("""
            SELECT SUM(amount_usd) FROM wise_tx
            WHERE date>=%s AND date<=%s AND category='Personal'
        """, (start, end))
        personal = float((cur.fetchone() or [0])[0] or 0)
        profit = revenue - biz_exp
        margin = (profit / revenue * 100) if revenue > 0 else 0
        return {"revenue": revenue, "leads": leads, "biz_exp": biz_exp,
                "personal": personal, "profit": profit, "margin": margin}
    except: return {}

# ── REPORTS ───────────────────────────────────────────────────────────────────
def daily_report(client):
    now   = datetime.now(EST)
    today = now.date()
    s     = period_summary(today, today)
    bals  = get_balances()
    by_cat = spending_by_category(today, today)

    msg  = f":sun_with_face: *Daily Report — {now.strftime('%B %d, %Y')}*\n\n"
    msg += f"*Revenue today:* ${s.get('revenue',0):,.2f} ({s.get('leads',0)} leads)\n"
    msg += f"*Business spend:* ${s.get('biz_exp',0):,.2f}\n"
    msg += f"*Profit:* ${s.get('profit',0):,.2f} | Margin: {s.get('margin',0):.1f}%\n"
    if by_cat:
        msg += "\n*Spending breakdown:*\n"
        for cat, total, cnt in by_cat:
            msg += f"  - {cat}: ${float(total):,.2f}\n"
    msg += "\n*Wise balances:*\n" + "\n".join(f"  - {b}" for b in bals)
    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"daily_report: {e}")

def weekly_report(client):
    now      = datetime.now(EST)
    week_end = now.date()
    week_start = week_end - timedelta(days=6)
    prev_start = week_start - timedelta(days=7)
    prev_end   = week_start - timedelta(days=1)

    curr = period_summary(week_start, week_end)
    prev = period_summary(prev_start, prev_end)
    by_cat  = spending_by_category(week_start, week_end)
    top_cust = revenue_by_customer(week_start, week_end)

    margin_change = curr.get("margin",0) - prev.get("margin",0)
    rev_change    = curr.get("revenue",0) - prev.get("revenue",0)
    personal_change = curr.get("personal",0) - prev.get("personal",0)

    msg  = f":bar_chart: *Weekly Report — {week_start} to {week_end}*\n\n"
    msg += f"*Revenue:* ${curr.get('revenue',0):,.2f} ({curr.get('leads',0)} leads)"
    if rev_change != 0:
        icon = ":arrow_up:" if rev_change > 0 else ":arrow_down:"
        msg += f" {icon} ${abs(rev_change):,.0f} vs last week"
    msg += f"\n*Business spend:* ${curr.get('biz_exp',0):,.2f}\n"
    msg += f"*Profit:* ${curr.get('profit',0):,.2f}\n"
    msg += f"*Margin:* {curr.get('margin',0):.1f}%"
    if margin_change != 0:
        icon = ":arrow_up:" if margin_change > 0 else ":arrow_down_small:"
        msg += f" {icon} {abs(margin_change):.1f}% vs last week"
        if margin_change < -5:
            msg += f"\n:warning: *Margin dropped {abs(margin_change):.1f}% — worth reviewing spend*"
    msg += "\n"
    if by_cat:
        msg += "\n*Spending by category:*\n"
        for cat, total, cnt in by_cat:
            msg += f"  - {cat}: ${float(total):,.2f} ({cnt} payments)\n"
    if personal_change > 100:
        msg += f"\n:eyes: *Personal expenses up ${personal_change:,.0f} vs last week*"
    elif personal_change < -100:
        msg += f"\n:white_check_mark: Personal expenses down ${abs(personal_change):,.0f} vs last week"
    if top_cust:
        msg += f"\n\n*Top revenue clients this week:*\n"
        for cust, cnt, net, first, last in top_cust[:5]:
            msg += f"  - {cust}: ${float(net):,.2f} ({cnt} charges)\n"
    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"weekly_report: {e}")

# ── AI ANSWER ─────────────────────────────────────────────────────────────────
def answer(q, client=None):
    log.info(f"Q: {q}")

    # Check if this is a natural language recipient identification
    # e.g. "Signal House is our SMS provider" or "Abdul Rehman Tahir bought me a macbook"
    if client and is_recipient_info(q):
        return handle_recipient_info(q)

    try: bals = get_balances()
    except: bals = []

    w_count, w_oldest, w_newest, w_total = wise_stats()
    f_count, f_oldest, f_newest, f_net   = fb_stats()

    now   = datetime.now(EST)
    today = now.date()
    month_start = today.replace(day=1)
    week_start  = today - timedelta(days=today.weekday())

    month = period_summary(month_start, today)
    week  = period_summary(week_start, today)

    by_cat_all   = spending_by_category()
    by_recip_all = spending_by_recipient(limit=30)
    by_cust      = revenue_by_customer()
    recent       = recent_wise(40)

    bal_text = "\n".join(f"  {b}" for b in bals) or "  unavailable"

    cat_text = "\n".join(
        f"  {r[0]}: ${float(r[1]):,.0f} ({r[2]} payments)"
        for r in by_cat_all) or "  none"

    recip_text = "\n".join(
        f"  {r[0]} [{r[3]}]: ${float(r[1]):,.0f} ({r[2]} payments)"
        for r in by_recip_all) or "  none"

    cust_text = "\n".join(
        f"  {r[0]}: {r[1]} charges | LTV ${float(r[2]):,.2f} | {r[3]} to {r[4]}"
        for r in by_cust) or "  none"

    recent_text = "\n".join(
        f"  {r[0]}: ${float(r[1]):,.0f} ({float(r[2]):,.0f} {r[3]}) -> {r[4]} [{r[5] or 'Unknown'}]"
        for r in recent) or "  none"

    system = f"""You are the financial intelligence bot for LeadsPilot — Suleman's SMS lead gen business ($75/lead, painting contractors USA).
Answer naturally and directly. Exact numbers. USD always. Calculate anything asked.

TODAY: {today} | This month: {month_start} to {today} | This week: {week_start} to {today}

WISE BALANCES (live):
{bal_text}

DATABASE: {w_count} Wise transactions ({w_oldest} to {w_newest})
DATABASE: {f_count} Fanbasis transactions ({f_oldest} to {f_newest})

THIS MONTH P&L:
  Revenue: ${month.get('revenue',0):,.2f} ({month.get('leads',0)} leads)
  Business expenses: ${month.get('biz_exp',0):,.2f}
  Personal: ${month.get('personal',0):,.2f}
  Profit: ${month.get('profit',0):,.2f} | Margin: {month.get('margin',0):.1f}%

THIS WEEK P&L:
  Revenue: ${week.get('revenue',0):,.2f} ({week.get('leads',0)} leads)
  Business expenses: ${week.get('biz_exp',0):,.2f}
  Profit: ${week.get('profit',0):,.2f} | Margin: {week.get('margin',0):.1f}%

ALL-TIME SPENDING BY CATEGORY:
{cat_text}

ALL-TIME SPENDING BY RECIPIENT:
{recip_text}

FANBASIS CLIENTS — LIFETIME VALUE:
{cust_text}

RECENT WISE TRANSACTIONS:
{recent_text}

BUSINESS CATEGORIES: SMS Cost, Data Provider, Salary, Software, Business Other, Rent
PERSONAL CATEGORIES: Personal, Hardware, Loan/Personal
INVESTMENT: Investment (Interactive Brokers — this is an asset not an expense)

For profit calculations: revenue minus business expenses only (not personal/investment).
For LTV: use the customer data above.
For margin: profit / revenue * 100."""

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":600,"system":system,
                  "messages":[{"role":"user","content":q}]},timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"Claude: {e}")
    return "Could not answer right now."

def is_recipient_info(text):
    """Detect if message is telling us about a recipient."""
    low = text.lower()
    patterns = ["is our", "is a ", "is my ", "is the ", "pays for", "we pay", "we sent",
                "salary", "provider", "software", "expense", "personal", "rent", "loan"]
    return any(p in low for p in patterns) and len(text.split()) < 30

def handle_recipient_info(text):
    """Use Claude to extract recipient info from natural language."""
    system = """Extract recipient information from this message.
Return JSON only: {"raw_name": "...", "clean_name": "...", "category": "...", "note": "..."}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Loan, Hardware, Business Other
If you can't extract clear info return: {"error": "unclear"}"""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":AI_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-haiku-4-5","max_tokens":200,"system":system,
                  "messages":[{"role":"user","content":text}]},timeout=15)
        if r.status_code == 200:
            result = r.json()["content"][0]["text"].strip()
            data = json.loads(re.sub(r'```\w*|```','',result).strip())
            if "error" not in data and data.get("raw_name"):
                save_recipient(data["raw_name"], data["clean_name"], data["category"], data.get("note",""))
                return (f":white_check_mark: Got it! *{data['raw_name']}* = "
                        f"*{data['clean_name']}* ({data['category']}). "
                        f"All transactions updated.")
    except: pass
    return None

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
def scheduler(client):
    """Background thread: daily 7pm EST, weekly Sunday 10am EST."""
    last_daily  = None
    last_weekly = None
    while True:
        time.sleep(60)
        try:
            now = datetime.now(EST)
            today = now.date()
            # Daily at 7pm EST
            if now.hour == 19 and now.minute < 2 and last_daily != today:
                daily_report(client)
                last_daily = today
            # Weekly on Sunday at 10am EST
            if now.weekday() == 6 and now.hour == 10 and now.minute < 2 and last_weekly != today:
                weekly_report(client)
                last_weekly = today
        except Exception as e: log.error(f"scheduler: {e}")

# ── WORKER ────────────────────────────────────────────────────────────────────
def worker(client):
    while True:
        time.sleep(180)
        try:
            log.info("Background sync...")
            unknowns = sync_wise_all()
            sync_fanbasis()
            if unknowns: ask_new_unknowns(unknowns, client)
        except Exception as e: log.error(f"worker: {e}")

# ── STARTUP ───────────────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_LeadsPilot Finance Bot loading — syncing all data..._")

        fb_count = sync_fanbasis()
        unknowns = sync_wise_all()

        w_count, w_oldest, w_newest, w_total = wise_stats()
        f_count, f_oldest, f_newest, f_net   = fb_stats()
        bals     = get_balances()
        by_cat   = spending_by_category()
        month    = period_summary(datetime.now(EST).date().replace(day=1), datetime.now(EST).date())

        # Get latest Wise transaction
        recent = recent_wise(1)
        latest_wise = ""
        if recent:
            r = recent[0]
            latest_wise = f"\n*Latest Wise transaction:* {r[0]} — ${float(r[1]):,.0f} to *{r[4]}*"

        # Get latest Fanbasis transaction
        db = get_db()
        latest_fb = ""
        if db:
            try:
                cur = db.cursor()
                cur.execute("SELECT date, customer, amount FROM fb_tx ORDER BY date DESC LIMIT 1")
                row = cur.fetchone()
                if row: latest_fb = f"\n*Latest Fanbasis charge:* {row[0]} — ${float(row[2]):,.0f} from *{row[1]}*"
            except: pass

        msg  = f"*LeadsPilot Finance Bot online* :white_check_mark:\n\n"
        msg += f"*Wise:* {w_count} transactions ({w_oldest} to {w_newest})\n"
        msg += f"*Fanbasis:* {f_count} transactions ({f_oldest} to {f_newest})\n"
        msg += f"*All-time net revenue:* ${float(f_net or 0):,.2f}\n"
        msg += f"*This month profit:* ${month.get('profit',0):,.2f} | Margin: {month.get('margin',0):.1f}%\n"
        msg += "\n*Wise balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if by_cat:
            msg += "\n\n*All-time spending by category:*\n"
            for cat, total, cnt in by_cat:
                msg += f"  - {cat}: ${float(total):,.2f}\n"
        msg += latest_wise + latest_fb
        msg += "\n\nAsk me anything — profit, margin, LTV, spending, revenue."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)

        # Ask about unknowns AFTER the main summary
        if unknowns:
            time.sleep(2)
            ask_new_unknowns(unknowns, client)

    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

# ── WEBHOOKS ──────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status": "LeadsPilot Finance Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    wc, _, _, wt = wise_stats(); fc, _, _, fn = fb_stats()
    return jsonify({"status":"ok","wise_tx":wc,"fb_tx":fc,"net_revenue":float(fn or 0)})

@flask_app.route("/webhook/fanbasis", methods=["POST"])
def fb_webhook():
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type","unknown")
        log.info(f"Fanbasis webhook: {event_type}")
        # Only care about disputes
        if "dispute" in event_type:
            buyer = payload.get("buyer",{}) or {}
            item  = payload.get("item",{}) or {}
            amt   = float(payload.get("amount",0))/100
            try:
                slack_app.client.chat_postMessage(
                    channel=CHANNEL_ID,
                    text=f":rotating_light: *Dispute Filed!*\n"
                         f"Customer: *{buyer.get('name','Unknown')}* ({buyer.get('email','')})\n"
                         f"Product: *{item.get('name','Unknown')}*\n"
                         f"Amount: *${amt:,.2f}*\n"
                         f"Action required — check Fanbasis dashboard.")
            except: pass
        # Silently save revenue
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

@flask_app.route("/webhook/wise", methods=["POST", "GET"])
def wise_webhook():
    if request.method == "GET":
        return jsonify({"status":"ok","message":"Wise webhook active"}), 200
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type","unknown")
        log.info(f"Wise webhook: {event_type}")
        # balances#update fires on every card payment and bank transfer
        if event_type == "balances#update":
            threading.Thread(target=handle_wise_balance_update,
                args=(payload,), daemon=True).start()
        elif event_type == "transfers#state-change":
            threading.Thread(target=handle_wise_transfer,
                args=(payload,), daemon=True).start()
        return jsonify({"status":"ok"}), 200
    except Exception as e:
        log.error(f"wise webhook: {e}")
        return jsonify({"status":"error"}), 500

def handle_wise_balance_update(payload):
    """
    balances#update fires on every debit/credit — card payments, transfers, fees.
    We trigger a targeted sync to pick up the new transaction immediately.
    """
    try:
        data = payload.get("data", {})
        tx_type = data.get("transaction_type","")
        if tx_type == "debit":
            # New spending — sync immediately
            log.info("Balance update (debit) — syncing now...")
            unknowns = sync_wise_all()
            if unknowns:
                ask_new_unknowns(unknowns, slack_app.client)
    except Exception as e:
        log.error(f"handle_wise_balance_update: {e}")

def handle_wise_transfer(payload):
    """transfers#state-change fires when a bank transfer completes."""
    try:
        data  = payload.get("data", {})
        state = data.get("current_state","")
        if state == "outgoing_payment_sent":
            log.info("Transfer completed — syncing now...")
            unknowns = sync_wise_all()
            if unknowns:
                ask_new_unknowns(unknowns, slack_app.client)
    except Exception as e:
        log.error(f"handle_wise_transfer: {e}")

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>","", text or "").strip()
    if not text: return
    say("_Checking..._")
    response = answer(text, slack_app.client)
    if response: say(response)

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text",""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
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
