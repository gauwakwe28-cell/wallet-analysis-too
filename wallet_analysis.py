import sys
print("=== SCRIPT LOADED ===", flush=True)

import os
import time
import psycopg2
import requests
from flask import Flask, jsonify

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")
TARGET_WALLET = os.environ.get("TARGET_WALLET")

print(f"Config loaded — DATABASE_URL set: {bool(DATABASE_URL)}, HELIUS_API_KEY set: {bool(HELIUS_API_KEY)}, TARGET_WALLET: {TARGET_WALLET}", flush=True)


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
    conn.commit()
    c.close()
    conn.close()
    print("init_db() completed", flush=True)


def get_current_market_cap(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            print(f"  DexScreener non-200 for {mint}: HTTP {resp.status_code}", flush=True)
            return None
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        fdv = pairs[0].get("fdv")
        return float(fdv) if fdv else None
    except Exception as e:
        print(f"get_current_market_cap error for {mint}: {e}", flush=True)
        return None


def get_market_caps_batch(mints):
    if not mints:
        return {}

    result = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            joined = ",".join(chunk)
            url = f"https://api.dexscreener.com/latest/dex/tokens/{joined}"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print(f"DexScreener batch non-200: HTTP {resp.status_code}", flush=True)
                continue
            data = resp.json()
            pairs = data.get("pairs") or []
            for pair in pairs:
                mint = pair.get("baseToken", {}).get("address")
                fdv = pair.get("fdv")
                if mint and fdv and mint not in result:
                    result[mint] = float(fdv)
        except Exception as e:
            print(f"get_market_caps_batch error on chunk: {e}", flush=True)
        time.sleep(0.5)

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
                if mint:
                    mints.add(mint)
    return list(mints)


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

    mints = extract_bought_mints(txs, TARGET_WALLET)
    print(f"Found {len(mints)} unique mints bought", flush=True)

    market_caps = get_market_caps_batch(mints)
    print(f"Got market cap data for {len(market_caps)} of {len(mints)} mints", flush=True)

    recorded_count = 0
    for mint in mints:
        try:
            c.execute("SELECT 1 FROM tracked_buys WHERE mint = %s AND wallet = %s", (mint, TARGET_WALLET))
            if c.fetchone():
                continue

            market_cap = market_caps.get(mint)
            if market_cap is None:
                print(f"  {mint}: no market cap data, skipping", flush=True)
                continue
            if market_cap >= 20000:
                print(f"  {mint}: market cap ${market_cap:,.0f} too high, skipping", flush=True)
                continue

            c.execute(
                "INSERT INTO tracked_buys (mint, wallet, market_cap_at_buy) VALUES (%s, %s, %s)",
                (mint, TARGET_WALLET, market_cap)
            )
            recorded_count += 1
            print(f"  RECORDED: {mint} at ${market_cap:,.0f}", flush=True)

        except Exception as e:
            print(f"  Error processing {mint}: {e}", flush=True)
            conn.rollback()
            continue

    conn.commit()
    c.close()
    conn.close()
    print(f"=== record_new_buys() finished — {recorded_count} new buys recorded ===", flush=True)


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
    """)
    ready = c.fetchall()
    print(f"{len(ready)} buys ready for outcome check", flush=True)

    for row_id, mint in ready:
        current_mc = get_current_market_cap(mint)
        c.execute(
            "UPDATE tracked_buys SET checked_final = TRUE, outcome_market_cap = %s, outcome_checked_at = NOW() WHERE id = %s",
            (current_mc, row_id)
        )
        print(f"  Checked: {mint} -> ${current_mc}", flush=True)

    conn.commit()
    c.close()
    conn.close()
    print("=== check_outcomes() finished ===", flush=True)


@app.route("/run-check", methods=["GET", "POST"])
def run_check():
    print(">>> /run-check endpoint HIT <<<", flush=True)
    record_new_buys()
    check_outcomes()
    return jsonify({"status": "done"})


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


@app.route("/")
def home():
    return "Wallet analysis tool running"


print("=== Calling init_db() ===", flush=True)
init_db()

if __name__ == "__main__":
    print("=== Starting Flask app ===", flush=True)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
