import os
import sqlite3
import requests
import time
import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")
TARGET_WALLET = os.environ.get("TARGET_WALLET")


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
            price_at_buy NUMERIC,
            checked_final BOOLEAN DEFAULT FALSE,
            outcome_market_cap NUMERIC,
            outcome_checked_at TIMESTAMP
        )
    """)
    conn.commit()
    c.close()
    conn.close()


def get_current_market_cap(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        return pairs[0].get("fdv")
    except Exception as e:
        print(f"Error fetching market cap for {mint}: {e}")
        return None


def get_wallet_recent_buys(wallet, limit=50):
    try:
        url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions"
        params = {"api-key": HELIUS_API_KEY, "limit": limit}
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return []
        return resp.json()
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return []


def extract_buys(transactions, wallet):
    mints_found = []
    for tx in transactions:
        for transfer in tx.get("tokenTransfers", []) or []:
            if transfer.get("toUserAccount") == wallet:
                mint = transfer.get("mint")
                if mint:
                    mints_found.append(mint)
    return list(set(mints_found))


def record_new_buys():
    if not TARGET_WALLET:
        print("No TARGET_WALLET configured")
        return

    conn = get_conn()
    c = conn.cursor()

    txs = get_wallet_recent_buys(TARGET_WALLET)
    mints = extract_buys(txs, TARGET_WALLET)

    for mint in mints:
        c.execute("SELECT 1 FROM tracked_buys WHERE mint = %s AND wallet = %s", (mint, TARGET_WALLET))
        if c.fetchone():
            continue

        market_cap = get_current_market_cap(mint)
        if market_cap is None or market_cap >= 20000:
            continue

        c.execute(
            "INSERT INTO tracked_buys (mint, wallet, market_cap_at_buy) VALUES (%s, %s, %s)",
            (mint, TARGET_WALLET, market_cap)
        )
        print(f"Recorded new buy: {mint} at ${market_cap:,.0f}")

    conn.commit()
    c.close()
    conn.close()


def check_outcomes():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        SELECT id, mint FROM tracked_buys
        WHERE checked_final = FALSE
        AND buy_time <= NOW() - INTERVAL '3 days'
    """)
    ready = c.fetchall()

    for row_id, mint in ready:
        current_mc = get_current_market_cap(mint)
        c.execute(
            "UPDATE tracked_buys SET checked_final = TRUE, outcome_market_cap = %s, outcome_checked_at = NOW() WHERE id = %s",
            (current_mc, row_id)
        )
        print(f"Checked outcome: {mint} → ${current_mc}")

    conn.commit()
    c.close()
    conn.close()


@app.route("/run-check", methods=["GET", "POST"])
def run_check():
    record_new_buys()
    check_outcomes()
    return jsonify({"status": "done"})


@app.route("/results")
def results():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT mint, market_cap_at_buy, outcome_market_cap, buy_time
        FROM tracked_buys
        WHERE checked_final = TRUE
        ORDER BY buy_time DESC
    """)
    rows = c.fetchall()
    c.close()
    conn.close()

    output = []
    for mint, entry_mc, exit_mc, buy_time in rows:
        mult = (float(exit_mc) / float(entry_mc)) if exit_mc and entry_mc else None
        output.append({
            "mint": mint,
            "entry_market_cap": float(entry_mc) if entry_mc else None,
            "exit_market_cap": float(exit_mc) if exit_mc else None,
            "multiplier": round(mult, 2) if mult else None,
            "buy_time": str(buy_time)
        })
    return jsonify(output)


@app.route("/")
def home():
    return "Wallet analysis tool running"


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
