"""
LeadsPilot Wise Bot — Uses Activity API
/v1/profiles/{pid}/activities = exact same transaction feed you see in Wise app
Every transaction: card, transfer, fee, conversion — all of it
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
EST             = ZoneInfo("America/New_York")
WISE_BASE       = "https://api.wise.com"

KNOWN = [
    ("Signal House SMS",                  "Signal House SMS",    "SMS Cost"),
    ("Pay*Signal House",                  "Signal House SMS",    "SMS Cost"),
    ("Sendivo",                           "Sendivo",             "SMS Cost"),
    ("Fanbasis.com",                      "Fanbasis (Jacob)",    "Data Provider"),
    ("Fanbasis",                          "Fanbasis (Jacob)",    "Data Provider"),
    ("ZEESHAN SHABBIR",                   "Zeeshan Shabbir",     "Data Provider"),
    ("Ghulam Shabir",                     "Zeeshan Shabbir",     "Data Provider"),
    ("Ghulam Shabir ",                    "Zeeshan Shabbir",     "Data Provider"),
    ("Muhammad Hisham",                   "Muhammad Hisham",     "Personal"),
    ("Muhammad Hisham ",                  "Muhammad Hisham",     "Personal"),
    ("Abdul Rehman",                      "Abdul Rehman",        "Personal"),
    ("Starla Shanaine Pamolarco Gadiano", "Starla",              "Salary"),
    ("Niña Selvendy Amor Cayongcong",     "Nina",                "Salary"),
    ("John Isaac Cane",                   "John",                "Salary"),
    ("Queenzen Alvarado Bensi",           "Queenzen",            "Salary"),
    ("Annalyn Budejas",                   "Annalyn",             "Salary"),
    ("INTERACTIVE BROKERS LLC",           "Interactive Brokers", "Investment"),
    ("Usman Ahmed",                       "Usman Ahmed",         "Rent"),
    ("Wahaj Khan",                        "Wahaj Khan",          "Loan/Personal"),
    ("shayan amir khan",                  "Wahaj Khan",          "Loan/Personal"),
    ("MOEEZ MAZHAR",                      "Moeez Mazhar",        "Hardware"),
    ("Abdul Rehman Tahir",                "Abdul Rehman Tahir",  "Hardware"),
    ("Pak Mac AC",                        "Pak Mac AC",          "Hardware"),
    ("Anthropic",                         "Anthropic",           "Software"),
    ("Claude",                            "Claude",              "Software"),
    ("Claude.ai Subscription",            "Claude",              "Software"),
    ("HighLevel",                         "HighLevel",           "Software"),
    ("Highlevel Inc.",                    "HighLevel",           "Software"),
    ("Highlevel Agency Sub",              "HighLevel",           "Software"),
    ("Calendly",                          "Calendly",            "Software"),
    ("Opus Virtual Offices",              "Opus Virtual Offices","Software"),
    ("Opus Virtual Offices Llc",          "Opus Virtual Offices","Software"),
    ("Zoom",                              "Zoom",                "Software"),
    ("Zoom.com 888-799-9666",             "Zoom",                "Software"),
    ("N8n Cloud1",                        "N8n Cloud",           "Software"),
    ("Paddle.net* N8n Cloud1",            "N8n Cloud",           "Software"),
    ("Slack",                             "Slack",               "Software"),
    ("Slack T06094wlhcy",                 "Slack",               "Software"),
    ("Framer",                            "Framer",              "Software"),
    ("Framer.com",                        "Framer",              "Software"),
    ("Google",                            "Google Workspace",    "Software"),
    ("Google Workspace_joinpilo",         "Google Workspace",    "Software"),
    ("Google*Workspace Joinp",            "Google Workspace",    "Software"),
    ("Retell AI",                         "Retell AI",           "Software"),
    ("Www.retellai.com",                  "Retell AI",           "Software"),
    ("Instantly",                         "Instantly",           "Software"),
    ("Whop Charan Invests",               "Whop",                "Software"),
    ("Whop*Charan Invests",               "Whop",                "Software"),
    ("Grasshopper Group Llc",             "Grasshopper",         "Software"),
    ("Grasshopper Group, Llc",            "Grasshopper",         "Software"),
    ("Bizee.com",                         "Bizee",               "Software"),
    ("Onlinejobs.ph",                     "OnlineJobs.ph",       "Software"),
    ("Onlinejobsph",                      "OnlineJobs.ph",       "Software"),
    ("Ow Mulebuy.com",                    "Mulebuy",             "Personal"),
    ("Ow *Mulebuy.com",                   "Mulebuy",             "Personal"),
    ("Whop Rinip Ventures Ll",            "Whop Rinip",          "Business Other"),
    ("Whop*Rinip Ventures Ll",            "Whop Rinip",          "Business Other"),
    ("Saurabh Kumar",                     "Saurabh Kumar",       "Business Other"),
    ("Saurabh",                           "Saurabh Kumar",       "Business Other"),
    ("Abdullah Habib (Minor)",            "Abdullah Habib",      "Business Other"),
    ("Inyxel Studios LLC",                "Inyxel Studios",      "Business Other"),
    ("Sp Thatonestreet",                  "Sp Thatonestreet",    "Business Other"),
    ("TransferWise",                      "TransferWise",        "Software"),
    ("Jonabelle Bayona Cahigas",          "Jonabelle",           "Unknown"),
    ("Gia Breeana Gentile",               "Gia Breeana",         "Unknown"),
    ("Divisible Inc",                     "Divisible Inc",       "SKIP"),
    ("LEADS PILOT LLC",                   "LeadsPilot",          "SKIP"),
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
    except Exception as e:
        log.error(f"DB: {e}"); _db = None; return None

def init_db():
    db = get_db()
    if not db: return
    db.cursor().execute("""
        DROP TABLE IF EXISTS tx;
        CREATE TABLE tx (
            id         TEXT PRIMARY KEY,
            date       DATE NOT NULL,
            amount_aud NUMERIC(14,4) NOT NULL,
            raw_name   TEXT,
            clean_name TEXT,
            category   TEXT,
            tx_type    TEXT,
            synced_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS known (
            raw_name   TEXT PRIMARY KEY,
            clean_name TEXT,
            category   TEXT
        );
        CREATE TABLE IF NOT EXISTS asked (
            raw_name TEXT PRIMARY KEY
        );
        CREATE INDEX IF NOT EXISTS idx_tx_date ON tx(date);
        CREATE INDEX IF NOT EXISTS idx_tx_cat  ON tx(category);
    """)
    for raw, clean, cat in KNOWN:
        try:
            db.cursor().execute(
                "INSERT INTO known(raw_name,clean_name,category) VALUES(%s,%s,%s) "
                "ON CONFLICT(raw_name) DO NOTHING", (raw, clean, cat))
            db.cursor().execute(
                "INSERT INTO asked(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw,))
        except: pass
    log.info("DB ready + seeded")

def lookup(raw_name):
    if not raw_name: return None, None
    db = get_db()
    if not db: return None, None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name,category FROM known WHERE raw_name=%s", (raw_name,))
        r = cur.fetchone()
        return (r[0], r[1]) if r else (None, None)
    except: return None, None

def learn(raw_name, clean_name, category):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO known(raw_name,clean_name,category) VALUES(%s,%s,%s) "
            "ON CONFLICT(raw_name) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category", (raw_name, clean_name, category))
        db.cursor().execute(
            "UPDATE tx SET clean_name=%s, category=%s WHERE raw_name=%s",
            (clean_name, category, raw_name))
        db.cursor().execute(
            "INSERT INTO asked(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw_name,))
    except Exception as e: log.error(f"learn: {e}")

def was_asked(raw_name):
    db = get_db()
    if not db: return False
    try:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM asked WHERE raw_name=%s", (raw_name,))
        return cur.fetchone() is not None
    except: return False

def mark_asked(raw_name):
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO asked(raw_name) VALUES(%s) ON CONFLICT DO NOTHING", (raw_name,))
    except: pass

def insert_tx(tx_id, date, amount_aud, raw_name, clean_name, category, tx_type):
    if category == "SKIP": return
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO tx(id,date,amount_aud,raw_name,clean_name,category,tx_type) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(id) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, amount_aud=EXCLUDED.amount_aud",
            (tx_id, date, float(amount_aud), raw_name, clean_name, category, tx_type))
    except Exception as e: log.error(f"insert_tx {tx_id}: {e}")

# ── QUERIES ───────────────────────────────────────────────────────────────────
def stats():
    db = get_db()
    if not db: return 0, None, None
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM tx WHERE category!='SKIP'")
        return cur.fetchone()
    except: return 0, None, None

def by_category(start=None, end=None):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = "SELECT COALESCE(category,'Unknown'), SUM(amount_aud), COUNT(*) FROM tx WHERE category!='SKIP'"
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += " GROUP BY COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def by_recipient(start=None, end=None):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        q = ("SELECT COALESCE(clean_name,raw_name), SUM(amount_aud), COUNT(*), "
             "COALESCE(category,'Unknown') FROM tx WHERE category!='SKIP'")
        p = []
        if start: q += " AND date>=%s"; p.append(start)
        if end:   q += " AND date<=%s"; p.append(end)
        q += " GROUP BY COALESCE(clean_name,raw_name),COALESCE(category,'Unknown') ORDER BY SUM(amount_aud) DESC LIMIT 30"
        cur.execute(q, p); return cur.fetchall()
    except: return []

def recent(n=40):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT date, amount_aud, COALESCE(clean_name,raw_name), "
            "COALESCE(category,'Unknown'), tx_type FROM tx "
            "WHERE category!='SKIP' ORDER BY date DESC, synced_at DESC LIMIT %s", (n,))
        return cur.fetchall()
    except: return []

def period(start, end):
    db = get_db()
    if not db: return {}
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT
              SUM(CASE WHEN category IN ('SMS Cost','Data Provider','Salary','Software','Business Other','Rent')
                  THEN amount_aud ELSE 0 END),
              SUM(CASE WHEN category='Personal' THEN amount_aud ELSE 0 END),
              SUM(CASE WHEN category!='SKIP' THEN amount_aud ELSE 0 END)
            FROM tx WHERE date>=%s AND date<=%s
        """, (start, end))
        r = cur.fetchone()
        return {"biz": float(r[0] or 0), "personal": float(r[1] or 0), "total": float(r[2] or 0)}
    except: return {}

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
        headers=wise_h(), params={"types": "STANDARD"}, timeout=10)
    r.raise_for_status()
    out = []
    for b in r.json():
        v = float(b.get("amount", {}).get("value", 0))
        c = b.get("amount", {}).get("currency", "")
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

def sync_activities():
    """
    Use /v1/profiles/{pid}/activities — this is exactly what Wise app shows.
    Returns ALL activity: card payments, transfers, fees, conversions.
    Has real merchant names exactly as shown in the app.
    """
    pid = get_pid()
    unknowns = []
    count = 0
    
    # Go back to Nov 2025
    since = datetime(2025, 11, 1, tzinfo=timezone.utc).isoformat()
    cursor = None
    
    while True:
        try:
            params = {"size": 100, "since": since}
            if cursor: params["cursor"] = cursor
            
            r = requests.get(f"{WISE_BASE}/v1/profiles/{pid}/activities",
                headers=wise_h(), params=params, timeout=20)
            log.info(f"Activities: HTTP {r.status_code}")
            
            if r.status_code == 200:
                data = r.json()
                activities = data.get("activities", [])
                log.info(f"  Got {len(activities)} activities")
                if not activities: break
                
                for act in activities:
                    # Only process money going OUT
                    primary = act.get("primaryAmount", {})
                    amount_val = float(primary.get("value", 0))
                    currency   = primary.get("currency", "AUD")
                    
                    # Skip positives (incoming) and non-AUD for now
                    if amount_val >= 0: continue
                    if currency != "AUD": continue
                    
                    amount_aud = abs(amount_val)
                    
                    # Get the title — this is exactly what Wise shows you
                    title = act.get("title", "")
                    resource = act.get("resource", {}) or {}
                    
                    # For transfers, get real recipient name
                    raw_name = title
                    if act.get("type") == "TRANSFER":
                        acct_id = resource.get("targetAccountId") or resource.get("accountId")
                        if acct_id:
                            resolved = resolve_name(acct_id)
                            if resolved: raw_name = resolved
                    
                    if not raw_name: continue
                    raw_name = str(raw_name).strip()
                    
                    # Skip internal/incoming
                    clean_check, cat_check = lookup(raw_name)
                    if cat_check == "SKIP": continue
                    if raw_name in ("Wise", "TransferWise") and amount_val > 0: continue
                    
                    date_s = act.get("createdAt") or act.get("created", "")
                    try: d = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
                    except: d = datetime.now().date()
                    
                    tx_id = str(act.get("id") or resource.get("id") or f"{date_s}_{amount_aud}")
                    tx_type = act.get("type", "CARD")
                    
                    clean, cat = lookup(raw_name)
                    if not clean and not was_asked(raw_name):
                        unknowns.append((raw_name, amount_aud, d))
                    
                    insert_tx(tx_id, d, amount_aud, raw_name, clean or raw_name, cat, tx_type)
                    count += 1
                
                # Pagination
                next_cursor = data.get("nextCursor") or data.get("cursor")
                if not next_cursor or len(activities) < 100: break
                cursor = next_cursor
                
            else:
                # Activity API not available — fall back to transfers + statements
                log.warning(f"Activity API returned {r.status_code}: {r.text[:100]}")
                log.info("Falling back to transfers + balance statements")
                unknowns += sync_transfers_fallback()
                unknowns += sync_statements_fallback()
                break
                
        except Exception as e:
            log.error(f"sync_activities: {e}")
            unknowns += sync_transfers_fallback()
            unknowns += sync_statements_fallback()
            break
    
    log.info(f"Activities sync: {count} transactions, {len(unknowns)} new unknowns")
    return unknowns

def sync_transfers_fallback():
    """Fallback: bank transfers via /v1/transfers"""
    pid = get_pid(); offset = 0; unknowns = []
    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(), params={"profile": pid, "limit": 100, "offset": offset}, timeout=20)
            r.raise_for_status()
            batch = r.json() if isinstance(r.json(), list) else r.json().get("content", [])
            if not batch: break
            for tx in batch:
                if tx.get("status") != "outgoing_payment_sent": continue
                amount = float(tx.get("sourceValue") or 0)
                if amount == 0: continue
                acct_id  = tx.get("targetAccount")
                raw_name = resolve_name(acct_id) if acct_id else None
                if not raw_name:
                    det = tx.get("details") or {}
                    raw_name = det.get("reference") or str(tx.get("id", ""))
                date_s = tx.get("created", "")
                try: d = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
                except: d = datetime.now().date()
                tx_id = str(tx.get("id", ""))
                clean, cat = lookup(raw_name)
                if not clean and not was_asked(raw_name):
                    unknowns.append((raw_name, amount, d))
                insert_tx(tx_id, d, amount, raw_name, clean or raw_name, cat, "TRANSFER")
            if len(batch) < 100: break
            offset += 100
        except Exception as e: log.error(f"transfers_fallback: {e}"); break
    return unknowns

def sync_statements_fallback():
    """Fallback: card transactions via balance statements"""
    pid = get_pid()
    now   = datetime.now(timezone.utc)
    start = datetime(2025, 11, 1, tzinfo=timezone.utc)
    unknowns = []
    try:
        r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
            headers=wise_h(), params={"types": "STANDARD"}, timeout=10)
        r.raise_for_status()
        balances = [(b["id"], b["amount"]["currency"]) for b in r.json() if b.get("id")]
    except: return unknowns
    
    for bal_id, currency in balances:
        chunk_end = now
        while chunk_end > start:
            chunk_start = max(chunk_end - timedelta(days=89), start)
            try:
                r = requests.get(
                    f"{WISE_BASE}/v1/profiles/{pid}/balance-statements/{bal_id}/statement.json",
                    headers=wise_h(),
                    params={"currency": currency,
                            "intervalStart": chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "intervalEnd":   chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                            "type": "COMPACT"}, timeout=25)
                if r.status_code != 200: break
                for tx in r.json().get("transactions", []):
                    val = float(tx.get("amount", {}).get("value", 0))
                    if val >= 0: continue
                    amount = abs(val)
                    det = tx.get("details", {}) or {}
                    # Safely extract name
                    parts = []
                    for key in ["merchant","senderName","description"]:
                        v = det.get(key)
                        if v and isinstance(v, str) and v.strip():
                            parts.append(v.strip()); break
                    recip = det.get("recipient")
                    if not parts and isinstance(recip, dict):
                        n = recip.get("name") or recip.get("accountHolderName")
                        if n: parts.append(str(n))
                    raw_name = parts[0] if parts else ""
                    if not raw_name: continue
                    clean_c, cat_c = lookup(raw_name)
                    if cat_c == "SKIP": continue
                    date_s = tx.get("date") or tx.get("createdAt", "")
                    try: d = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
                    except: d = datetime.now().date()
                    ext_id = str(tx.get("referenceNumber") or f"stmt_{bal_id}_{date_s}_{amount}")
                    clean, cat = lookup(raw_name)
                    if not clean and not was_asked(raw_name):
                        unknowns.append((raw_name, amount, d))
                    insert_tx(ext_id, d, amount, raw_name, clean or raw_name, cat, "CARD")
            except Exception as e: log.error(f"stmt chunk: {e}")
            chunk_end = chunk_start
    return unknowns

# ── ASK / CONFIRM ─────────────────────────────────────────────────────────────
def ask_unknowns(unknowns, client):
    seen = {}
    for raw_name, aud, d in unknowns:
        if raw_name not in seen: seen[raw_name] = (aud, d)
    to_ask = [(n, u, d) for n, (u, d) in seen.items()
              if lookup(n)[0] is None and not was_asked(n)]
    if not to_ask: return
    for raw, _, _ in to_ask: mark_asked(raw)
    if len(to_ask) == 1:
        raw, aud, d = to_ask[0]
        msg = (f":question: *New transaction — who is this?*\n"
               f"*`{raw}`* — {aud:,.2f} AUD on {d}\n\n"
               f"Just reply naturally e.g. _{raw} is our SMS provider_")
    else:
        msg = f":question: *{len(to_ask)} new recipients:*\n\n"
        for raw, aud, d in to_ask[:20]:
            msg += f"• *`{raw}`* — {aud:,.2f} AUD ({d})\n"
        msg += "\nJust tell me who each one is naturally."
    try: client.chat_postMessage(channel=CHANNEL_ID, text=msg)
    except Exception as e: log.error(f"ask: {e}")

# ── AI ────────────────────────────────────────────────────────────────────────
def is_learning(text):
    low = text.lower()
    return (any(t in low for t in ["is our","is a ","is my ","is the ","salary",
                                    "provider","expense","personal","rent","loan",
                                    "he is","she is","bought","build","software"])
            and len(text.split()) < 35)

def handle_learning(text):
    system = """Extract recipient info from this message. Return JSON only:
{"raw_name":"exact name as shown in Wise","clean_name":"human name","category":"category"}
Categories: SMS Cost, Data Provider, Salary, Software, Personal, Investment, Rent, Loan/Personal, Hardware, Business Other
If unclear: {"error":"unclear"}"""
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": AI_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5", "max_tokens": 150,
                  "system": system, "messages": [{"role": "user", "content": text}]},
            timeout=15)
        if r.status_code == 200:
            data = json.loads(r.json()["content"][0]["text"].strip())
            if "error" not in data and data.get("raw_name"):
                learn(data["raw_name"], data["clean_name"], data["category"])
                return (f":white_check_mark: *`{data['raw_name']}`* = "
                        f"*{data['clean_name']}* ({data['category']}). All past transactions updated.")
    except: pass
    return None

def answer(q):
    log.info(f"Q: {q}")
    try: bals = get_balances()
    except: bals = []
    count, oldest, newest = stats()
    now         = datetime.now(EST)
    today       = now.date()
    month_start = today.replace(day=1)
    week_start  = today - timedelta(days=today.weekday())
    month = period(month_start, today)
    week  = period(week_start, today)
    cats  = by_category()
    recips = by_recipient()
    recips_month = by_recipient(month_start, today)
    last  = recent(40)

    bal_text   = "\n".join(f"  {b}" for b in bals) or "  unavailable"
    cat_text   = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD ({r[2]})" for r in cats) or "  none"
    rec_text   = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips) or "  none"
    rec_month  = "\n".join(f"  {r[0]} [{r[3]}]: {float(r[1]):,.2f} AUD ({r[2]})" for r in recips_month) or "  none"
    last_text  = "\n".join(f"  {r[0]}: {float(r[1]):,.2f} AUD -> {r[2]} [{r[3]}]" for r in last) or "  none"

    system = f"""You are the Wise spending bot for LeadsPilot — Suleman's AUD business account.
Answer directly. All amounts in AUD.

IMPORTANT NAME MATCHING RULES:
- Signal House = Pay*Signal House = Signal House SMS — always combine these
- HighLevel = Highlevel Inc. = Highlevel Agency Sub — always combine
- Google = Google Workspace = Google*Workspace — always combine
- Wahaj Khan = shayan amir khan — same person (cousin). Payments include: loan given, pass-through for his clients (NOT our money), MacBook advance. Not all Wahaj payments are our actual expense.
- Fanbasis = Fanbasis.com = Fanbasis (Jacob) — same data provider

TODAY: {today} | Month: {month_start} | Week: {week_start}

BALANCES (live):
{bal_text}

DB: {count} transactions ({oldest} to {newest})

THIS MONTH:
  Business: {month.get('biz',0):,.2f} AUD
  Personal: {month.get('personal',0):,.2f} AUD
  Total: {month.get('total',0):,.2f} AUD

THIS WEEK:
  Business: {week.get('biz',0):,.2f} AUD
  Personal: {week.get('personal',0):,.2f} AUD

ALL-TIME BY CATEGORY:
{cat_text}

ALL-TIME BY RECIPIENT:
{rec_text}

THIS MONTH BY RECIPIENT:
{rec_month}

RECENT TRANSACTIONS (newest first):
{last_text}"""

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": AI_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5", "max_tokens": 500,
                  "system": system, "messages": [{"role": "user", "content": q}]},
            timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e: log.error(f"AI: {e}")
    return "Could not answer right now."

# ── STARTUP + WORKER ──────────────────────────────────────────────────────────
def startup(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_Wise Bot loading — syncing all transactions from Wise..._")
        unknowns = sync_activities()
        count, oldest, newest = stats()
        bals  = get_balances()
        cats  = by_category()
        last  = recent(3)
        msg   = f"*Wise Bot online* :white_check_mark:\n"
        msg  += f"*{count} transactions* ({oldest} to {newest})\n"
        msg  += "*Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if cats:
            msg += "\n\n*All-time by category:*\n"
            for cat, total, cnt in cats:
                msg += f"  - {cat}: {float(total):,.2f} AUD ({cnt})\n"
        if last:
            msg += "\n*Recent transactions:*\n"
            for r in last:
                msg += f"  - {r[0]}: {float(r[1]):,.2f} AUD → *{r[2]}* [{r[3]}]\n"
        msg += "\nAsk me anything."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)
        if unknowns:
            time.sleep(2)
            ask_unknowns(unknowns, client)
    except Exception as e:
        log.error(f"startup: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(180)
        try:
            unknowns = sync_activities()
            if unknowns: ask_unknowns(unknowns, client)
        except Exception as e: log.error(f"worker: {e}")

# ── FLASK ─────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/", methods=["GET"])
def root(): return jsonify({"status": "LeadsPilot Wise Bot running"}), 200

@flask_app.route("/health", methods=["GET"])
def health():
    count, oldest, newest = stats()
    return jsonify({"status": "ok", "transactions": count,
                    "oldest": str(oldest), "newest": str(newest)})

@flask_app.route("/webhook/wise", methods=["POST", "GET"])
def wise_webhook():
    if request.method == "GET": return jsonify({"status": "ok"}), 200
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type", "")
        log.info(f"Wise webhook: {event_type}")
        threading.Thread(target=lambda: ask_unknowns(sync_activities(), slack_app.client),
            daemon=True).start()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"webhook: {e}"); return jsonify({"status": "error"}), 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>", "", text or "").strip()
    if not text: return
    if is_learning(text):
        r = handle_learning(text)
        if r: say(r); return
    say("_Checking..._")
    say(answer(text))

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text", ""), say)

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
