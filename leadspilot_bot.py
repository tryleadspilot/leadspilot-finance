"""
LeadsPilot Financial Intelligence Bot — COMBINED
================================================
Revenue:  Fanbasis API (transactions + webhooks)
Spending: Wise API (card transactions + bank transfers + webhooks)

KEY FEATURE: Smart recipient learning
- Every new transaction with an unknown recipient → bot asks you in Slack
- You reply: "that is Signal House SMS cost" or "personal expense"
- Bot saves it forever — never asks again
- All questions answered by Claude with full context

DB Tables:
  wise_tx        — all Wise transactions (card + transfer)
  fb_tx          — all Fanbasis revenue transactions
  recipients     — learned recipient names + categories
  pending_review — transactions waiting for your confirmation
"""

import os, re, json, logging, requests, threading, time, ssl, urllib.parse
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
WISE_KEY        = os.environ["WISE_API_KEY"]
FB_KEY          = os.environ["FANBASIS_API_KEY"]
AI_KEY          = os.environ["ANTHROPIC_API_KEY"]
DATABASE_URL    = os.environ.get("DATABASE_URL", "")
CHANNEL_ID      = os.environ.get("CHANNEL_ID", "C0AUJHKE5C1")
PORT            = int(os.environ.get("PORT", 8080))

WISE_BASE = "https://api.wise.com"
FB_BASE   = "https://www.fanbasis.com/public-api"

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
            log.info("DB connected")
        return _db
    except ImportError:
        log.error("pg8000 not installed"); return None
    except Exception as e:
        log.error(f"DB: {e}"); _db = None; return None

def init_db():
    db = get_db()
    if not db: log.warning("No DB"); return
    db.cursor().execute("""
        -- Wise transactions (card + transfer)
        CREATE TABLE IF NOT EXISTS wise_tx (
            id          TEXT PRIMARY KEY,
            date        DATE,
            amount_raw  NUMERIC(14,4),
            currency    TEXT,
            amount_usd  NUMERIC(14,2),
            direction   TEXT,
            type        TEXT,
            raw_name    TEXT,
            recipient   TEXT,
            category    TEXT,
            confirmed   BOOLEAN DEFAULT FALSE,
            synced_at   TIMESTAMP DEFAULT NOW()
        );

        -- Fanbasis revenue
        CREATE TABLE IF NOT EXISTS fb_tx (
            id          TEXT PRIMARY KEY,
            date        DATE,
            customer    TEXT,
            email       TEXT,
            product     TEXT,
            amount      NUMERIC(14,2),
            fee         NUMERIC(14,2),
            net         NUMERIC(14,2),
            synced_at   TIMESTAMP DEFAULT NOW()
        );

        -- Learned recipient knowledge base
        CREATE TABLE IF NOT EXISTS recipients (
            raw_name    TEXT PRIMARY KEY,
            clean_name  TEXT,
            category    TEXT,
            note        TEXT,
            learned_at  TIMESTAMP DEFAULT NOW()
        );

        -- Transactions waiting for your review
        CREATE TABLE IF NOT EXISTS pending_review (
            tx_id       TEXT PRIMARY KEY,
            source      TEXT,
            raw_name    TEXT,
            amount_usd  NUMERIC(14,2),
            date        DATE,
            asked_at    TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_wise_date   ON wise_tx(date);
        CREATE INDEX IF NOT EXISTS idx_wise_recip  ON wise_tx(recipient);
        CREATE INDEX IF NOT EXISTS idx_wise_cat    ON wise_tx(category);
        CREATE INDEX IF NOT EXISTS idx_fb_date     ON fb_tx(date);
        CREATE INDEX IF NOT EXISTS idx_fb_customer ON fb_tx(customer);
    """)
    log.info("DB ready")

# ── DB HELPERS ────────────────────────────────────────────────────────────────
def db_exec(sql, params=()):
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute(sql, params)
        return cur
    except Exception as e:
        log.error(f"db_exec: {e}"); return None

def get_recipient(raw_name):
    """Look up learned recipient info."""
    if not raw_name: return None
    db = get_db()
    if not db: return None
    try:
        cur = db.cursor()
        cur.execute("SELECT clean_name, category, note FROM recipients WHERE raw_name=%s", (raw_name,))
        return cur.fetchone()
    except: return None

def save_recipient(raw_name, clean_name, category, note=""):
    """Save learned recipient forever."""
    db = get_db()
    if not db: return
    try:
        db.cursor().execute(
            "INSERT INTO recipients(raw_name,clean_name,category,note) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(raw_name) DO UPDATE SET clean_name=EXCLUDED.clean_name, "
            "category=EXCLUDED.category, note=EXCLUDED.note",
            (raw_name, clean_name, category, note))
        # Update all existing transactions with this recipient
        db.cursor().execute(
            "UPDATE wise_tx SET recipient=%s, category=%s, confirmed=TRUE WHERE raw_name=%s",
            (clean_name, category, raw_name))
        log.info(f"Learned: {raw_name} -> {clean_name} ({category})")
    except Exception as e:
        log.error(f"save_recipient: {e}")

def add_pending(tx_id, source, raw_name, amount_usd, date):
    db_exec(
        "INSERT INTO pending_review(tx_id,source,raw_name,amount_usd,date) VALUES(%s,%s,%s,%s,%s) "
        "ON CONFLICT(tx_id) DO NOTHING",
        (tx_id, source, raw_name, amount_usd, date))

def remove_pending(tx_id):
    db_exec("DELETE FROM pending_review WHERE tx_id=%s", (tx_id,))

def get_pending():
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("SELECT tx_id, source, raw_name, amount_usd, date FROM pending_review ORDER BY date DESC")
        return cur.fetchall()
    except: return []

def upsert_wise(tx_id, date, raw, cur_code, usd, direction, tx_type, raw_name, recipient, category, confirmed):
    db_exec(
        "INSERT INTO wise_tx(id,date,amount_raw,currency,amount_usd,direction,type,raw_name,recipient,category,confirmed) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT(id) DO UPDATE SET recipient=EXCLUDED.recipient, category=EXCLUDED.category, "
        "amount_usd=EXCLUDED.amount_usd, confirmed=EXCLUDED.confirmed",
        (tx_id, date, raw, cur_code, usd, direction, tx_type, raw_name, recipient, category, confirmed))

def upsert_fb(tx_id, date, customer, email, product, amount, fee, net):
    db_exec(
        "INSERT INTO fb_tx(id,date,customer,email,product,amount,fee,net) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET amount=EXCLUDED.amount",
        (tx_id, date, customer, email, product, amount, fee, net))

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
            _fx[cur] = {"AUD":0.64,"GBP":1.27,"EUR":1.08,"CAD":0.73,"PKR":0.0036,"PHP":0.017}.get(cur, 1.0)
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

def get_wise_balances():
    pid = get_pid()
    r = requests.get(f"{WISE_BASE}/v4/profiles/{pid}/balances",
        headers=wise_h(), params={"types": "STANDARD"}, timeout=10)
    r.raise_for_status()
    out = []
    for b in r.json():
        v = float(b.get("amount", {}).get("value", 0))
        c = b.get("amount", {}).get("currency", "")
        if v > 0:
            usd = to_usd(v, c)
            out.append(f"{c}: {v:,.2f}" + (f" (~${usd:,.0f} USD)" if c != "USD" else ""))
    return out

def get_card_tokens():
    """Get all Wise card tokens for this profile."""
    pid = get_pid()
    try:
        r = requests.get(f"{WISE_BASE}/v3/spend/profiles/{pid}/cards",
            headers=wise_h(), timeout=10)
        if r.status_code == 200:
            cards = r.json() if isinstance(r.json(), list) else r.json().get("data", [])
            return [c.get("cardToken") for c in cards if c.get("cardToken")]
    except Exception as e:
        log.error(f"get_card_tokens: {e}")
    return []

def sync_wise_card_transactions():
    """
    Sync ALL card transactions using the Card Transaction API.
    Loops through 90-day chunks from today back to Nov 2025.
    Returns real merchant names (Sendivo, Anthropic, Signal House etc).
    """
    pid    = get_pid()
    tokens = get_card_tokens()
    if not tokens:
        log.warning("No card tokens found")
        return 0

    total   = 0
    new_ask = []
    # Go back to account start (Nov 2025) in 89-day chunks
    now     = datetime.now(timezone.utc)
    start   = datetime(2025, 11, 17, tzinfo=timezone.utc)

    for token in tokens:
        log.info(f"Syncing card transactions for token {token[:8]}...")
        chunk_end = now
        while chunk_end > start:
            chunk_start = max(chunk_end - timedelta(days=89), start)
            last_id = None

            while True:
                params = {
                    "fromCreationTime": chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "toCreationTime":   chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "pageSize": 100
                }
                if last_id: params["lastId"] = last_id

                try:
                    r = requests.get(
                        f"{WISE_BASE}/v4/spend/profiles/{pid}/cards/{token}/transactions",
                        headers=wise_h(), params=params, timeout=20)
                    log.info(f"Card tx {chunk_start.date()} to {chunk_end.date()}: HTTP {r.status_code}")
                    if r.status_code != 200: break

                    txs = r.json().get("transactions", [])
                    if not txs: break

                    for tx in txs:
                        state = tx.get("state", "")
                        if state not in ("COMPLETED",): continue

                        tx_id    = str(tx.get("id", ""))
                        tx_type  = tx.get("type", "ECOM_PURCHASE")
                        merchant = tx.get("merchant", {}) or {}
                        raw_name = merchant.get("name", "Unknown")

                        # Billing amount = what left your account in AUD
                        billing  = tx.get("billingAmount", {}) or {}
                        raw      = float(billing.get("amount", 0))
                        cur_code = billing.get("currency", "AUD")
                        usd      = to_usd(raw, cur_code)

                        date_s = tx.get("creationTime", "")
                        try:
                            date = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
                        except:
                            date = datetime.now().date()

                        # Check if we know this recipient
                        known = get_recipient(raw_name)
                        if known:
                            recipient, category, _ = known
                            confirmed = True
                        else:
                            recipient = raw_name
                            category  = None
                            confirmed = False
                            # Queue for review if we haven't asked yet
                            new_ask.append((tx_id, "card", raw_name, usd, date))

                        upsert_wise(tx_id, date, raw, cur_code, usd,
                                    "out", tx_type, raw_name, recipient, category, confirmed)
                        total += 1

                    # Pagination - use last ID
                    if len(txs) < 100: break
                    last_id = txs[-1].get("id")
                    if not last_id: break

                except Exception as e:
                    log.error(f"card_tx chunk: {e}"); break

            chunk_end = chunk_start

    log.info(f"Card sync: {total} transactions")
    return total, new_ask

def sync_wise_transfers():
    """Sync all bank transfers via /v1/transfers."""
    pid    = get_pid()
    offset = 0
    total  = 0
    new_ask = []

    while True:
        try:
            r = requests.get(f"{WISE_BASE}/v1/transfers",
                headers=wise_h(),
                params={"profile": pid, "limit": 100, "offset": offset},
                timeout=20)
            r.raise_for_status()
            data  = r.json()
            batch = data if isinstance(data, list) else data.get("content", [])
            if not batch: break

            for tx in batch:
                if tx.get("status") not in ("outgoing_payment_sent",): continue
                raw      = float(tx.get("sourceValue") or tx.get("targetValue") or 0)
                cur_code = tx.get("sourceCurrency", "AUD")
                usd      = to_usd(raw, cur_code)
                det      = tx.get("details") or {}
                raw_name = det.get("reference") or tx.get("reference") or f"transfer-{tx.get('id','')}"
                date_s   = tx.get("created") or tx.get("createdAt", "")
                try:
                    date = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
                except:
                    date = datetime.now().date()
                tx_id = str(tx.get("id", ""))

                known = get_recipient(raw_name)
                if known:
                    recipient, category, _ = known
                    confirmed = True
                else:
                    recipient = raw_name
                    category  = None
                    confirmed = False
                    new_ask.append((tx_id, "transfer", raw_name, usd, date))

                upsert_wise(tx_id, date, raw, cur_code, usd,
                            "out", "TRANSFER", raw_name, recipient, category, confirmed)
                total += 1

            if len(batch) < 100: break
            offset += 100
        except Exception as e:
            log.error(f"transfers: {e}"); break

    log.info(f"Transfer sync: {total}")
    return total, new_ask

def sync_fanbasis():
    """Sync all Fanbasis revenue transactions."""
    page = 1; total = 0
    while True:
        try:
            r = requests.get(f"{FB_BASE}/checkout-sessions/transactions",
                headers={"x-api-key": FB_KEY}, params={"page": page, "per_page": 100}, timeout=20)
            if r.status_code != 200: break
            data = r.json().get("data", {})
            txs  = data.get("transactions", []) if isinstance(data, dict) else []
            if not txs: break
            for tx in txs:
                tx_id    = str(tx.get("id", ""))
                fan      = tx.get("fan", {}) or {}
                service  = tx.get("service", {}) or {}
                customer = fan.get("name", "Unknown")
                email    = fan.get("email", "")
                product  = service.get("title", "Unknown")
                amount   = float(service.get("price", 0))
                fee      = float(tx.get("fee_amount", 0))
                net      = float(tx.get("net_amount", amount - fee))
                date_s   = tx.get("transaction_date") or tx.get("created_at", "")
                try: date = datetime.fromisoformat(str(date_s).replace("Z", "+00:00")).date()
                except: date = datetime.now().date()
                if tx_id: upsert_fb(tx_id, date, customer, email, product, amount, fee, net); total += 1
            if not data.get("pagination", {}).get("has_more", False): break
            page += 1
        except Exception as e:
            log.error(f"fanbasis sync: {e}"); break
    log.info(f"Fanbasis sync: {total}")
    return total

# ── RECIPIENT REVIEW FLOW ─────────────────────────────────────────────────────
def ask_about_recipients(new_asks, client):
    """
    For each new unknown recipient, ask Suleman in Slack.
    Groups duplicates — only asks once per unique raw_name.
    """
    seen = set()
    for tx_id, source, raw_name, usd, date in new_asks:
        if raw_name in seen: continue
        seen.add(raw_name)
        # Check DB cache first
        if get_recipient(raw_name): continue
        add_pending(tx_id, source, raw_name, usd, date)
        try:
            client.chat_postMessage(
                channel=CHANNEL_ID,
                text=(f":question: *New transaction I don't recognise:*\n"
                      f"*Recipient:* `{raw_name}`\n"
                      f"*Amount:* ${usd:,.2f} USD\n"
                      f"*Date:* {date}\n\n"
                      f"Who is this? Reply:\n"
                      f"`learn {raw_name} | Clean Name | Category | Note`\n\n"
                      f"*Categories:* SMS Cost, Data Provider, Salary, Software, "
                      f"Personal, Investment, Rent, Loan, Hardware, Business Other"))
        except Exception as e:
            log.error(f"ask_recipient: {e}")

def process_learn_command(text, say):
    """
    Handle: learn Signal House SMS | Signal House SMS | SMS Cost | our SMS provider
    Saves the mapping forever.
    """
    text = text.strip()
    if not text.lower().startswith("learn "): return False
    rest = text[6:].strip()
    parts = [p.strip() for p in rest.split("|")]
    if len(parts) < 3:
        say("Format: `learn Raw Name | Clean Name | Category | Optional note`\n"
            "Example: `learn SG SMS LTD | Signal House SMS | SMS Cost | our primary SMS provider`")
        return True
    raw_name   = parts[0]
    clean_name = parts[1]
    category   = parts[2]
    note       = parts[3] if len(parts) > 3 else ""
    save_recipient(raw_name, clean_name, category, note)
    say(f":white_check_mark: Learned!\n*`{raw_name}`* is now *{clean_name}* ({category})\nAll past transactions updated.")
    return True

# ── DB QUERIES ────────────────────────────────────────────────────────────────
def db_wise_stats():
    db = get_db()
    if not db: return 0, None, None, 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), SUM(amount_usd) FROM wise_tx WHERE direction='out'")
        return cur.fetchone()
    except: return 0, None, None, 0

def db_fb_stats():
    db = get_db()
    if not db: return 0, None, None, 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*), MIN(date), MAX(date), SUM(net) FROM fb_tx")
        return cur.fetchone()
    except: return 0, None, None, 0

def db_spending_by_category():
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT COALESCE(category,'Unknown'), SUM(amount_usd), COUNT(*)
            FROM wise_tx WHERE direction='out'
            GROUP BY COALESCE(category,'Unknown')
            ORDER BY SUM(amount_usd) DESC
        """)
        return cur.fetchall()
    except: return []

def db_spending_by_recipient():
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT COALESCE(recipient, raw_name), SUM(amount_usd), COUNT(*),
                   COALESCE(category,'Unknown')
            FROM wise_tx WHERE direction='out'
            GROUP BY COALESCE(recipient, raw_name), COALESCE(category,'Unknown')
            ORDER BY SUM(amount_usd) DESC LIMIT 30
        """)
        return cur.fetchall()
    except: return []

def db_fb_by_customer():
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT customer, COUNT(*) purchases, SUM(net) net,
                   MIN(date) first, MAX(date) last
            FROM fb_tx GROUP BY customer ORDER BY SUM(net) DESC LIMIT 25
        """)
        return cur.fetchall()
    except: return []

def db_recent_wise(n=30):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT date, amount_usd, currency, amount_raw,
                   COALESCE(recipient, raw_name), category, type
            FROM wise_tx WHERE direction='out'
            ORDER BY date DESC, synced_at DESC LIMIT %s
        """, (n,))
        return cur.fetchall()
    except: return []

def db_recent_fb(n=20):
    db = get_db()
    if not db: return []
    try:
        cur = db.cursor()
        cur.execute("SELECT date, customer, product, amount, net FROM fb_tx ORDER BY date DESC LIMIT %s", (n,))
        return cur.fetchall()
    except: return []

def db_unconfirmed_count():
    db = get_db()
    if not db: return 0
    try:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM wise_tx WHERE confirmed=FALSE")
        return cur.fetchone()[0]
    except: return 0

# ── AI ────────────────────────────────────────────────────────────────────────
def answer(q):
    log.info(f"Q: {q}")
    try: bals = get_wise_balances()
    except: bals = []

    w_count, w_oldest, w_newest, w_total = db_wise_stats()
    f_count, f_oldest, f_newest, f_net   = db_fb_stats()
    pending     = get_pending()
    by_cat      = db_spending_by_category()
    by_recip    = db_spending_by_recipient()
    by_customer = db_fb_by_customer()
    recent_wise = db_recent_wise(40)
    recent_fb   = db_recent_fb(20)
    unconfirmed = db_unconfirmed_count()

    bal_text = "\n".join(f"  {b}" for b in bals) or "  unavailable"

    spend_cat = "\n".join(
        f"  {r[0]}: ${float(r[1]):,.0f} ({r[2]} transactions)"
        for r in by_cat) or "  none"

    spend_recip = "\n".join(
        f"  {r[0]} [{r[3]}]: ${float(r[1]):,.0f} ({r[2]} payments)"
        for r in by_recip) or "  none"

    revenue_customers = "\n".join(
        f"  {r[0]}: {r[1]} purchases | ${float(r[2]):,.2f} net | "
        f"first: {r[3]} | last: {r[4]}"
        for r in by_customer) or "  none"

    recent_wise_text = "\n".join(
        f"  {r[0]}: ${float(r[1]):,.0f} ({float(r[3]):,.0f} {r[2]}) -> {r[4]} [{r[5] or 'Unknown'}]"
        for r in recent_wise) or "  none"

    recent_fb_text = "\n".join(
        f"  {r[0]}: {r[1]} | {r[2]} | ${float(r[3]):,.2f}"
        for r in recent_fb) or "  none"

    system = f"""You are the financial intelligence bot for LeadsPilot — Suleman's SMS lead gen business.
Answer naturally, directly, with exact numbers. USD always.
Calculate profit, margins, LTV, weekly/monthly breakdowns as needed.

=== WISE BALANCES (live) ===
{bal_text}

=== WISE SPENDING DATABASE: {w_count} transactions ({w_oldest} to {w_newest}) ===
Total spent all time: ${float(w_total or 0):,.2f} USD
Unconfirmed recipients: {unconfirmed} (waiting for Suleman to identify)
Pending review: {len(pending)} transactions

SPENDING BY CATEGORY:
{spend_cat}

SPENDING BY RECIPIENT (all time):
{spend_recip}

RECENT WISE TRANSACTIONS:
{recent_wise_text}

=== FANBASIS REVENUE DATABASE: {f_count} transactions ({f_oldest} to {f_newest}) ===
Total net revenue all time: ${float(f_net or 0):,.2f} USD

TOP CUSTOMERS BY LIFETIME VALUE:
{revenue_customers}

RECENT REVENUE:
{recent_fb_text}

=== BUSINESS CONTEXT ===
Business: LeadsPilot — SMS lead gen, painting contractors USA, $75/lead
Data providers: Fanbasis.com (Jacob), ZEESHAN SHABBIR / Ghulam Shabir
SMS providers: Signal House SMS, Sendivo
Staff: Starla, Nina, Queenzen, John, Annalyn (Filipino VAs)
Personal spend: Muhammad Hisham, Abdul Rehman (Suleman himself)
Investment: Interactive Brokers LLC
Rent: Usman Ahmed

Answer any question about profit, margin, LTV, spending, revenue.
For 'profit' = Fanbasis revenue minus Wise business expenses (not personal/investment).
For 'what did we spend on X' = filter by recipient or category."""

    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": AI_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5", "max_tokens": 600,
                  "system": system, "messages": [{"role": "user", "content": q}]},
            timeout=30)
        if r.status_code == 200:
            return r.json()["content"][0]["text"].strip()
    except Exception as e:
        log.error(f"Claude: {e}")
    return "Could not answer right now."

# ── STARTUP ───────────────────────────────────────────────────────────────────
def full_sync(client):
    try:
        client.chat_postMessage(channel=CHANNEL_ID,
            text="_LeadsPilot Financial Bot loading — syncing all data..._")

        # Sync Fanbasis revenue
        fb_count = sync_fanbasis()

        # Sync Wise card transactions
        card_result = sync_wise_card_transactions()
        card_count  = card_result[0] if isinstance(card_result, tuple) else card_result
        card_asks   = card_result[1] if isinstance(card_result, tuple) else []

        # Sync Wise bank transfers
        transfer_result = sync_wise_transfers()
        tx_count   = transfer_result[0] if isinstance(transfer_result, tuple) else transfer_result
        tx_asks    = transfer_result[1] if isinstance(transfer_result, tuple) else []

        # Ask about unknown recipients
        all_asks = card_asks + tx_asks
        if all_asks:
            ask_about_recipients(all_asks, client)

        # Post startup summary
        w_count, w_oldest, w_newest, w_total = db_wise_stats()
        f_count, f_oldest, f_newest, f_net   = db_fb_stats()
        bals      = get_wise_balances()
        by_cat    = db_spending_by_category()
        unconfirmed = db_unconfirmed_count()

        msg  = f"*LeadsPilot Financial Bot online* :white_check_mark:\n\n"
        msg += f"*Wise DB:* {w_count} transactions ({w_oldest} to {w_newest})\n"
        msg += f"*Fanbasis DB:* {f_count} transactions ({f_oldest} to {f_newest})\n"
        msg += f"*Net Revenue all time:* ${float(f_net or 0):,.2f}\n"
        msg += f"*Total Wise spending:* ${float(w_total or 0):,.2f}\n"
        msg += "\n*Wise Balances:*\n" + "\n".join(f"  - {b}" for b in bals)
        if by_cat:
            msg += "\n\n*Spending by category (all time):*\n"
            for cat, total, cnt in by_cat[:6]:
                msg += f"  - {cat}: ${float(total):,.0f} ({cnt} payments)\n"
        if unconfirmed > 0:
            msg += f"\n:question: *{unconfirmed} transactions have unrecognised recipients* — check above messages to identify them."
        msg += "\n\nAsk me anything — profit, LTV, spending, revenue, margin."
        client.chat_postMessage(channel=CHANNEL_ID, text=msg)

    except Exception as e:
        log.error(f"full_sync: {e}")
        try: client.chat_postMessage(channel=CHANNEL_ID, text=f"Startup error: {e}")
        except: pass

def worker(client):
    while True:
        time.sleep(900)
        try:
            log.info("Background sync...")
            card_result     = sync_wise_card_transactions()
            transfer_result = sync_wise_transfers()
            fb_count        = sync_fanbasis()
            all_asks = []
            if isinstance(card_result, tuple): all_asks += card_result[1]
            if isinstance(transfer_result, tuple): all_asks += transfer_result[1]
            if all_asks: ask_about_recipients(all_asks, client)
        except Exception as e:
            log.error(f"worker: {e}")

# ── WEBHOOKS ──────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/webhook/wise", methods=["POST"])
def wise_webhook():
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type", payload.get("type", "unknown"))
        log.info(f"Wise webhook: {event_type}")
        # Card transaction state change — save to DB silently
        if "card" in event_type.lower() or "transaction" in event_type.lower():
            threading.Thread(target=handle_wise_webhook, args=(payload,), daemon=True).start()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"wise webhook: {e}"); return jsonify({"status": "error"}), 500

def handle_wise_webhook(payload):
    # Extract transaction data from webhook
    data     = payload.get("data", payload)
    tx_id    = str(data.get("id", data.get("transactionId", "")))
    merchant = data.get("merchant", {}) or {}
    raw_name = merchant.get("name", "Unknown")
    billing  = data.get("billingAmount", {}) or {}
    raw      = float(billing.get("amount", 0))
    cur_code = billing.get("currency", "AUD")
    usd      = to_usd(raw, cur_code)
    date_s   = data.get("creationTime", data.get("created", ""))
    try:
        date = datetime.fromisoformat(date_s.replace("Z", "+00:00")).date()
    except:
        date = datetime.now().date()
    known = get_recipient(raw_name)
    if known:
        recipient, category, _ = known
        confirmed = True
    else:
        recipient = raw_name
        category  = None
        confirmed = False
        ask_about_recipients([(tx_id, "card", raw_name, usd, date)], slack_app.client)
    if tx_id:
        upsert_wise(tx_id, date, raw, cur_code, usd, "out", "ECOM_PURCHASE", raw_name, recipient, category, confirmed)

@flask_app.route("/webhook/fanbasis", methods=["POST"])
def fb_webhook():
    try:
        payload    = request.get_json(force=True) or {}
        event_type = payload.get("event_type", "unknown")
        log.info(f"Fanbasis webhook: {event_type}")
        if event_type in ("payment.succeeded", "product.purchased", "subscription.renewed"):
            buyer  = payload.get("buyer", {}) or {}
            item   = payload.get("item", {}) or {}
            tx_id  = str(payload.get("payment_id") or payload.get("id", ""))
            amount = float(payload.get("amount", 0)) / 100
            upsert_fb(tx_id, datetime.now().date(), buyer.get("name","Unknown"),
                      buyer.get("email",""), item.get("name","Unknown"), amount, 0, amount)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"fb webhook: {e}"); return jsonify({"status": "error"}), 500

@flask_app.route("/health", methods=["GET"])
def health():
    wc, _, _, wt = db_wise_stats()
    fc, _, _, fn = db_fb_stats()
    return jsonify({"wise_tx": wc, "wise_total": float(wt or 0),
                    "fb_tx": fc, "fb_net": float(fn or 0)})

# ── SLACK ─────────────────────────────────────────────────────────────────────
slack_app = App(token=SLACK_BOT_TOKEN)

def process(text, say):
    text = re.sub(r"<@[^>]+>", "", text or "").strip()
    if not text: return
    # Check for learn command first
    if process_learn_command(text, say): return
    # Check for pending command
    if text.lower() in ("pending", "unknown", "unconfirmed"):
        rows = get_pending()
        if not rows:
            say(":white_check_mark: No pending recipients — everything is identified!")
            return
        msg = f":question: *{len(rows)} unidentified recipients:*\n\n"
        for tx_id, source, raw_name, usd, date in rows[:10]:
            msg += f"• `{raw_name}` — ${float(usd):,.0f} on {date}\n"
            msg += f"  Reply: `learn {raw_name} | Clean Name | Category | Note`\n\n"
        say(msg); return
    # Everything else — Claude
    say("_Checking..._")
    say(answer(text))

@slack_app.event("app_mention")
def on_mention(event, say): process(event.get("text", ""), say)

@slack_app.event("message")
def on_message(event, say):
    if event.get("bot_id") or event.get("subtype"): return
    t = (event.get("text") or "").strip()
    if t: process(t, say)

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)

if __name__ == "__main__":
    log.info("Starting LeadsPilot Financial Bot...")
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    threading.Timer(5, full_sync, args=[slack_app.client]).start()
    threading.Thread(target=worker, args=[slack_app.client], daemon=True).start()
    handler.start()
