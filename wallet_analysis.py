import sys
print("=== SCRIPT LOADED ===", flush=True)

import os
import time
import threading
import psycopg2
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")
TARGET_WALLET = os.environ.get("TARGET_WALLET")

print(f"Config loaded — DATABASE_URL set: {bool(DATABASE_URL)}, HELIUS_API_KEY set: {bool(HELIUS_API_KEY)}, TARGET_WALLET: {TARGET_WALLET}", flush=True)

BLOCKED_MINTS = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}

_dex_lock = threading.Lock()
_next_allowed_request_time = 0
_market_cap_cache = {}

_job_lock = threading.Lock()
_job_running = False


def _wait_for_rate_limit(min_interval=2.5, retry_after=0):
    global _next_allowed_request_time
    with _dex_lock:
        now = time.time()
        if retry_after:
            ban_until = now + retry_after
            if ban_until > _next_allowed_request_time:
                _next_allowed_request_time = ban_until
                print(f"  [RATE LIMIT] Global backoff set: {retry_after}s", flush=True)

        wait_until = max(_next_allowed_request_time, now + min_interval)
        sleep_time = wait_until - now
        if sleep_time > 0:
            time.sleep(sleep_time)
        _next_allowed_request_time = time.time() + min_interval


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS tracked_buys (
            id SERIAL PRIMARY KEY,
            mint TEXT,
            wallet TEXT,
            buy_time TIMESTAMP DEFAULT NOW(),
            market_cap_at_buy NUMERIC,
            checked_final BOOLEAN DEFAULT FALSE,
            outcome_market_cap NUMERIC,
            outcome_checked_at TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS processed_signatures (
            signature TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    c.close()
    conn.close()
    print("init_db() completed", flush=True)


def get_current_market_cap(mint, retries=3):
    now = time.time()
    if mint in _market_cap_cache:
        cached_at, value = _market_cap_cache[mint]
        if now - cached_at < 90:
            return value if value is not False else None

    try:
        _wait_for_rate_limit(min_interval=2.5)
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=8)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            print(f"  DexScreener 429 for {mint}, global backoff {retry_after}s", flush=True)
            _wait_for_rate_limit(retry_after=retry_after)
            if retries > 0:
                return get_current_market_cap(mint, retries=retries - 1)
            _market_cap_cache[mint] = (now, False)
            return None

        if resp.status_code != 200:
            print(f"  DexScreener non-200 for {mint}: HTTP {resp.status_code}", flush=True)
            return None

        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            _market_cap_cache[mint] = (now, False)
            return None

        for pair in pairs:
            fdv = pair.get("fdv")
            if fdv is not None and fdv != "":
                value = float(fdv)
                _market_cap_cache[mint] = (now, value)
                return value

        _market_cap_cache[mint] = (now, False)
        return None
    except Exception as e:
        print(f"get_current_market_cap error for {mint}: {e}", flush=True)
        return None


def get_market_caps_batch(mints):
    if not mints:
        return {}

    result = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        chunk_set = set(chunk)
        try:
            _wait_for_rate_limit(min_interval=2.5)
            joined = ",".join(chunk)
            url = f"https://api.dexscreener.com/latest/dex/tokens/{joined}"
            resp = requests.get(url, timeout=10)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"DexScreener batch 429, global backoff {retry_after}s", flush=True)
                _wait_for_rate_limit(retry_after=retry_after)
                resp = requests.get(url, timeout=10)
                if resp.status_code == 429:
                    print(f"DexScreener batch 429 on retry, skipping chunk", flush=True)
                    continue

            if resp.status_code == 200:
                data = resp.json()
                pairs = data.get("pairs") or []
                for pair in pairs:
                    fdv = pair.get("fdv")
                    if fdv is None or fdv == "":
                        continue

                    base_mint = pair.get("baseToken", {}).get("address")
                    quote_mint = pair.get("quoteToken", {}).get("address")

                    if base_mint and base_mint in chunk_set and base_mint not in result:
                        result[base_mint] = float(fdv)
                        _market_cap_cache[base_mint] = (time.time(), float(fdv))

                    if quote_mint and quote_mint in chunk_set and quote_mint not in result:
                        result[quote_mint] = float(fdv)
                        _market_cap_cache[quote_mint] = (time.time(), float(fdv))
            else:
                print(f"DexScreener batch non-200: HTTP {resp.status_code}", flush=True)
        except Exception as e:
            print(f"get_market_caps_batch error on chunk: {e}", flush=True)

    missing = [
        m for m in mints
        if m not in result and _market_cap_cache.get(m, (0, False))[1] is not False
    ]
    if missing:
        print(f"Batch missed {len(missing)} mints, retrying individually", flush=True)
        for mint in missing:
            mc = get_current_market_cap(mint)
            if mc is not None:
                result[mint] = mc

    return result


def get_wallet_recent_transactions(wallet, limit=50):
    try:
        url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions"
        params = {"api-key": HELIUS_API_KEY, "limit": limit}
        resp = requests.get(url, params=params, timeout=15)
        print(f"Helius response status: {resp.status_code}", flush=True)
        if resp.status_code != 200:
            print(f"Helius error body: {resp.text[:300]}", flush=True)
            return []
        return resp.json()
    except Exception as e:
        print(f"get_wallet_recent_transactions error: {e}", flush=True)
        return []


def extract_bought_mints(transactions, wallet):
    mints = set()
    for tx in transactions:
        for transfer in tx.get("tokenTransfers", []) or []:
            if transfer.get("toUserAccount") == wallet:
                mint = transfer.get("mint")
                if mint and mint not in BLOCKED_MINTS:
                    mints.add(mint)
    return list(mints)


def is_signature_processed(signature):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT 1 FROM processed_signatures WHERE signature = %s", (signature,))
    exists = c.fetchone() is not None
    c.close()
    conn.close()
    return exists


def mark_signature_processed(signature):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO processed_signatures (signature) VALUES (%s) ON CONFLICT DO NOTHING", (signature,))
        conn.commit()
    except Exception:
        pass
    c.close()
    conn.close()


def process_pending_mints():
    print("=== process_pending_mints() starting ===", flush=True)
    try:
        conn = get_conn()
        c = conn.cursor()
    except Exception as e:
        print(f"DATABASE CONNECTION FAILED: {e}", flush=True)
        return

    c.execute("""
        SELECT id, mint FROM tracked_buys
        WHERE market_cap_at_buy IS NULL AND wallet = %s
    """, (TARGET_WALLET,))
    pending = c.fetchall()
    print(f"{len(pending)} pending mints to process", flush=True)

    if not pending:
        c.close()
        conn.close()
        return

    mints = [row[1] for row in pending]
    market_caps = get_market_caps_batch(mints)
    print(f"Resolved market caps for {len(market_caps)} of {len(mints)} pending mints", flush=True)

    kept = 0
    dropped = 0
    for row_id, mint in pending:
        mc = market_caps.get(mint)

        if mc is None:
            print(f"  {mint}: no market cap data, dropping", flush=True)
            c.execute("DELETE FROM tracked_buys WHERE id = %s", (row_id,))
            dropped += 1
        elif mc <= 5000 or mc > 20000:
            print(f"  {mint}: market cap ${mc:,.0f} outside range (5000-20000), dropping", flush=True)
            c.execute("DELETE FROM tracked_buys WHERE id = %s", (row_id,))
            dropped += 1
        else:
            c.execute(
                "UPDATE tracked_buys SET market_cap_at_buy = %s WHERE id = %s",
                (mc, row_id)
            )
            kept += 1
            print(f"  RESOLVED: {mint} at ${mc:,.0f}", flush=True)

    conn.commit()
    c.close()
    conn.close()
    print(f"=== process_pending_mints() finished — {kept} kept, {dropped} dropped ===", flush=True)


def record_new_buys():
    print("=== record_new_buys() starting ===", flush=True)

    if not TARGET_WALLET:
        print("ERROR: TARGET_WALLET is not set", flush=True)
        return
    if not HELIUS_API_KEY:
        print("ERROR: HELIUS_API_KEY is not set", flush=True)
        return
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is not set", flush=True)
        return

    print(f"Using wallet: {TARGET_WALLET}", flush=True)

    try:
        conn = get_conn()
        c = conn.cursor()
        print("Database connected", flush=True)
    except Exception as e:
        print(f"DATABASE CONNECTION FAILED: {e}", flush=True)
        return

    txs = get_wallet_recent_transactions(TARGET_WALLET)
    print(f"Fetched {len(txs)} transactions", flush=True)

    if not txs:
        print("No transactions returned — stopping here", flush=True)
        c.close()
        conn.close()
        return

    recorded_count = 0
    skipped_count = 0

    for tx in txs:
        signature = tx.get("signature")
        if not signature:
            continue

        if is_signature_processed(signature):
            skipped_count += 1
            continue

        mints = extract_bought_mints([tx], TARGET_WALLET)
        if not mints:
            mark_signature_processed(signature)
            continue

        for mint in mints:
            try:
                c.execute("SELECT 1 FROM tracked_buys WHERE mint = %s AND wallet = %s", (mint, TARGET_WALLET))
                if c.fetchone():
                    continue

                c.execute(
                    "INSERT INTO tracked_buys (mint, wallet, market_cap_at_buy) VALUES (%s, %s, NULL)",
                    (mint, TARGET_WALLET)
                )
                recorded_count += 1
                print(f"  RECORDED (pending): {mint}", flush=True)

            except Exception as e:
                print(f"  Error processing {mint}: {e}", flush=True)
                conn.rollback()
                continue

        mark_signature_processed(signature)

    conn.commit()
    c.close()
    conn.close()
    print(f"=== record_new_buys() finished — {recorded_count} new, {skipped_count} sigs skipped ===", flush=True)


def record_buys_batch(mints):
    unique_mints = list(set(mints))
    if not unique_mints:
        return

    conn = get_conn()
    try:
        c = conn.cursor()

        new_mints = []
        for mint in unique_mints:
            c.execute("SELECT 1 FROM tracked_buys WHERE mint = %s AND wallet = %s", (mint, TARGET_WALLET))
            if not c.fetchone():
                new_mints.append(mint)

        if not new_mints:
            c.close()
            conn.close()
            return

        for mint in new_mints:
            c.execute(
                "INSERT INTO tracked_buys (mint, wallet, market_cap_at_buy) VALUES (%s, %s, NULL)",
                (mint, TARGET_WALLET)
            )
            print(f"WEBHOOK RECORDED (pending): {mint}", flush=True)

        conn.commit()
        c.close()
    except Exception as e:
        print(f"record_buys_batch error: {e}", flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def check_outcomes():
    print("=== check_outcomes() starting ===", flush=True)
    try:
        conn = get_conn()
        c = conn.cursor()
    except Exception as e:
        print(f"DATABASE CONNECTION FAILED: {e}", flush=True)
        return

    c.execute("""
        SELECT id, mint FROM tracked_buys
        WHERE checked_final = FALSE
        AND buy_time <= NOW() - INTERVAL '3 days'
        AND market_cap_at_buy IS NOT NULL
    """)
    ready = c.fetchall()
    print(f"{len(ready)} buys ready for outcome check", flush=True)

    if not ready:
        c.close()
        conn.close()
        print("=== check_outcomes() finished — nothing to check ===", flush=True)
        return

    ids_and_mints = ready
    mints = [row[1] for row in ids_and_mints]
    market_caps = get_market_caps_batch(mints)
    print(f"Resolved {len(market_caps)} outcome market caps in batch", flush=True)

    for row_id, mint in ids_and_mints:
        current_mc = market_caps.get(mint)
        c.execute(
            "UPDATE tracked_buys SET checked_final = TRUE, outcome_market_cap = %s, outcome_checked_at = NOW() WHERE id = %s",
            (current_mc, row_id)
        )
        print(f"  Checked: {mint} -> ${current_mc}", flush=True)

    conn.commit()
    c.close()
    conn.close()
    print("=== check_outcomes() finished ===", flush=True)


def run_all_checks():
    global _job_running
    try:
        process_pending_mints()
        record_new_buys()
        check_outcomes()
    except Exception as e:
        print(f"Background check error: {e}", flush=True)
    finally:
        with _job_lock:
            _job_running = False
        print("=== Background job finished ===", flush=True)


@app.route("/run-check", methods=["GET", "POST"])
def run_check():
    global _job_running
    print(">>> /run-check endpoint HIT <<<", flush=True)

    with _job_lock:
        if _job_running:
            return jsonify({"status": "already_running"}), 200
        _job_running = True

    thread = threading.Thread(target=run_all_checks, daemon=True)
    thread.start()

    return jsonify({"status": "started"}), 202


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    if not data:
        return "no data", 400

    transactions = data if isinstance(data, list) else [data]
    print(f"Webhook received {len(transactions)} transaction(s)", flush=True)

    all_mints = []
    for tx in transactions:
        if not isinstance(tx, dict):
            continue
        mints = extract_bought_mints([tx], TARGET_WALLET)
        all_mints.extend(mints)

    if all_mints:
        record_buys_batch(all_mints)

    return "ok", 200


@app.route("/cleanup-null-entries", methods=["GET", "POST"])
def cleanup_null_entries():
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM tracked_buys WHERE market_cap_at_buy IS NULL")
    deleted = c.rowcount
    conn.commit()
    c.close()
    conn.close()
    return jsonify({"deleted": deleted})


@app.route("/results")
def results():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT mint, market_cap_at_buy, outcome_market_cap, buy_time, checked_final
        FROM tracked_buys
        ORDER BY buy_time DESC
    """)
    rows = c.fetchall()
    c.close()
    conn.close()

    output = []
    for mint, entry_mc, exit_mc, buy_time, checked in rows:
        mult = None
        if exit_mc and entry_mc:
            mult = round(float(exit_mc) / float(entry_mc), 2)
        output.append({
            "mint": mint,
            "entry_market_cap": float(entry_mc) if entry_mc else None,
            "exit_market_cap": float(exit_mc) if exit_mc else None,
            "multiplier": mult,
            "buy_time": str(buy_time),
            "checked_final": checked
        })
    return jsonify(output)


@app.route("/live-pending")
def live_pending():
    """Fetch live market caps for all pending tokens right now."""
    if not TARGET_WALLET:
        return jsonify({"error": "TARGET_WALLET not set"}), 500

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, mint, market_cap_at_buy, buy_time
        FROM tracked_buys
        WHERE checked_final = FALSE
        AND market_cap_at_buy IS NOT NULL
        AND wallet = %s
        ORDER BY buy_time DESC
    """, (TARGET_WALLET,))
    rows = c.fetchall()
    c.close()
    conn.close()

    if not rows:
        return jsonify({"pending": [], "count": 0})

    mints = [row[1] for row in rows]
    current_caps = get_market_caps_batch(mints)

    results = []
    for row_id, mint, entry_mc, buy_time in rows:
        current_mc = current_caps.get(mint)
        mult = None
        if current_mc and entry_mc:
            mult = round(float(current_mc) / float(entry_mc), 2)

        results.append({
            "mint": mint,
            "entry_market_cap": float(entry_mc) if entry_mc else None,
            "current_market_cap": float(current_mc) if current_mc else None,
            "multiplier": mult,
            "status": "up" if mult and mult > 1 else ("down" if mult and mult < 1 else "flat"),
            "buy_time": str(buy_time)
        })

    return jsonify({"pending": results, "count": len(results)})


@app.route("/summary")
def summary():
    """Compact summary of pending positions. Easy to copy-paste."""
    if not TARGET_WALLET:
        return jsonify({"error": "TARGET_WALLET not set"}), 500

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT mint, market_cap_at_buy
        FROM tracked_buys
        WHERE checked_final = FALSE
        AND market_cap_at_buy IS NOT NULL
        AND wallet = %s
    """, (TARGET_WALLET,))
    rows = c.fetchall()
    c.close()
    conn.close()

    if not rows:
        return jsonify({
            "total_pending": 0,
            "up": 0,
            "down": 0,
            "flat": 0,
            "dead_or_no_data": 0,
            "avg_multiplier": None,
            "best": None,
            "worst": None
        })

    mints = [row[0] for row in rows]
    entry_map = {row[0]: float(row[1]) for row in rows}
    current_caps = get_market_caps_batch(mints)

    up = down = flat = dead = 0
    multipliers = []
    best_mint = worst_mint = None
    best_mult = -999999
    worst_mult = 999999

    for mint, entry_mc in entry_map.items():
        current_mc = current_caps.get(mint)
        if current_mc is None:
            dead += 1
            continue

        mult = current_mc / entry_mc
        multipliers.append(mult)

        if mult > 1:
            up += 1
        elif mult < 1:
            down += 1
        else:
            flat += 1

        if mult > best_mult:
            best_mult = mult
            best_mint = mint
        if mult < worst_mult:
            worst_mult = mult
            worst_mint = mint

    avg_mult = round(sum(multipliers) / len(multipliers), 2) if multipliers else None

    return jsonify({
        "total_pending": len(rows),
        "up": up,
        "down": down,
        "flat": flat,
        "dead_or_no_data": dead,
        "avg_multiplier": avg_mult,
        "best": {
            "mint": best_mint,
            "entry": entry_map.get(best_mint),
            "current": current_caps.get(best_mint),
            "multiplier": round(best_mult, 2)
        } if best_mint else None,
        "worst": {
            "mint": worst_mint,
            "entry": entry_map.get(worst_mint),
            "current": current_caps.get(worst_mint),
            "multiplier": round(worst_mult, 2)
        } if worst_mint else None
    })


@app.route("/")
def home():
    return "Wallet analysis tool running"


print("=== Calling init_db() ===", flush=True)
init_db()

if __name__ == "__main__":
    print("=== Starting Flask app ===", flush=True)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
