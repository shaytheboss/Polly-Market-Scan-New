"""
Polymarket Daily Scanner
Finds top gainers by absolute $ volume and % probability change (7d).
Saves results to SQLite database.
"""

import sqlite3
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional
import urllib.request
import urllib.error

# ─── Config ───────────────────────────────────────────────────────────────────

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DB_PATH = os.getenv("DB_PATH", "polymarket.db")
TOP_N   = int(os.getenv("TOP_N", "20"))

# ─── Database setup ───────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,
            scanned_at    TEXT NOT NULL,
            markets_total INTEGER
        );

        CREATE TABLE IF NOT EXISTS top_gainers_volume (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id       INTEGER REFERENCES daily_scans(id),
            rank          INTEGER,
            market_id     TEXT,
            question      TEXT,
            slug          TEXT,
            volume_24h    REAL,
            volume_total  REAL,
            yes_price     REAL,
            end_date      TEXT,
            url           TEXT
        );

        CREATE TABLE IF NOT EXISTS top_movers_pct (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id         INTEGER REFERENCES daily_scans(id),
            rank            INTEGER,
            market_id       TEXT,
            question        TEXT,
            slug            TEXT,
            yes_price_now   REAL,
            price_change_7d REAL,
            volume_24h      REAL,
            end_date        TEXT,
            url             TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scan_date   ON daily_scans(scan_date);
        CREATE INDEX IF NOT EXISTS idx_gainer_scan ON top_gainers_volume(scan_id);
        CREATE INDEX IF NOT EXISTS idx_mover_scan  ON top_movers_pct(scan_id);
    """)
    conn.commit()

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 30) -> dict | list:
    req = urllib.request.Request(url, headers={"User-Agent": "polymarket-scanner/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

def fetch_paginated(base_url: str, limit: int = 100) -> list[dict]:
    results = []
    offset = 0
    while True:
        url = f"{base_url}&limit={limit}&offset={offset}"
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"  Warning: fetch error at offset {offset}: {e}", file=sys.stderr)
            break

        if not data:
            break

        if isinstance(data, list):
            results.extend(data)
            if len(data) < limit:
                break
            offset += limit
        elif isinstance(data, dict) and "data" in data:
            results.extend(data["data"])
            if len(data["data"]) < limit:
                break
            offset += limit
        else:
            break

    return results

# ─── Data fetching ────────────────────────────────────────────────────────────

def fetch_active_markets() -> list[dict]:
    print("Fetching active markets from Gamma API...")
    url = f"{GAMMA_API_BASE}/markets?active=true&closed=false"
    markets = fetch_paginated(url)
    print(f"  Got {len(markets)} active markets")
    return markets

# ─── Processing ───────────────────────────────────────────────────────────────

def parse_market(m: dict) -> Optional[dict]:
    try:
        vol_24h   = float(m.get("volume24hr") or 0)
        vol_total = float(m.get("volume") or 0)

        # Current YES price
        op = m.get("outcomePrices")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except Exception:
                op = []
        yes_price = None
        if isinstance(op, list) and len(op) >= 1:
            try:
                yes_price = float(op[0])
            except (ValueError, TypeError):
                pass
        if yes_price is None:
            yes_price = float(m.get("lastTradePrice") or 0)

        # 7-day price change — comes directly from the API, no extra call needed
        price_change_7d = float(m.get("oneWeekPriceChange") or 0)

        market_id = str(m.get("id") or m.get("conditionId") or "")
        slug      = m.get("slug") or ""
        question  = m.get("question") or ""
        end_date  = m.get("endDateIso") or m.get("endDate") or ""
        url       = f"https://polymarket.com/event/{slug}" if slug else ""

        return {
            "market_id":       market_id,
            "question":        question,
            "slug":            slug,
            "volume_24h":      vol_24h,
            "volume_total":    vol_total,
            "yes_price":       yes_price,
            "price_change_7d": price_change_7d,
            "end_date":        end_date,
            "url":             url,
        }
    except Exception as e:
        print(f"  Parse error on market {m.get('id','?')}: {e}", file=sys.stderr)
        return None

def top_by_volume(markets: list[dict], n: int) -> list[dict]:
    valid = [m for m in markets if m["volume_24h"] > 0]
    return sorted(valid, key=lambda m: m["volume_24h"], reverse=True)[:n]

def top_by_pct_change(markets: list[dict], n: int) -> list[dict]:
    # Only consider markets with meaningful volume to filter out spam/dust markets
    valid = [m for m in markets if m["volume_24h"] > 1000 and m["price_change_7d"] != 0]
    return sorted(valid, key=lambda m: abs(m["price_change_7d"]), reverse=True)[:n]

# ─── Database writes ──────────────────────────────────────────────────────────

def save_scan(conn: sqlite3.Connection, markets: list[dict], vol_top: list[dict], pct_top: list[dict]):
    now = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_scans (scan_date, scanned_at, markets_total) VALUES (?,?,?)",
        (now.strftime("%Y-%m-%d"), now.isoformat(), len(markets))
    )
    scan_id = cur.lastrowid

    for rank, m in enumerate(vol_top, 1):
        cur.execute("""
            INSERT INTO top_gainers_volume
              (scan_id, rank, market_id, question, slug, volume_24h, volume_total,
               yes_price, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, rank, m["market_id"], m["question"], m["slug"],
              m["volume_24h"], m["volume_total"], m["yes_price"], m["end_date"], m["url"]))

    for rank, m in enumerate(pct_top, 1):
        cur.execute("""
            INSERT INTO top_movers_pct
              (scan_id, rank, market_id, question, slug,
               yes_price_now, price_change_7d, volume_24h, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, rank, m["market_id"], m["question"], m["slug"],
              m["yes_price"], m["price_change_7d"], m["volume_24h"], m["end_date"], m["url"]))

    conn.commit()
    return scan_id

# ─── Report ───────────────────────────────────────────────────────────────────

def print_report(vol_top: list[dict], pct_top: list[dict]):
    print("\n" + "="*70)
    print(f"  POLYMARKET DAILY SCAN — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*70)

    print(f"\n🏆 TOP {len(vol_top)} BY 24H VOLUME (USD)\n")
    for r, m in enumerate(vol_top, 1):
        price_str = f"{m['yes_price']*100:.1f}%" if m["yes_price"] else "N/A"
        print(f"  {r:>2}. ${m['volume_24h']:>12,.0f}  [{price_str}]  {m['question'][:65]}")

    print(f"\n📈 TOP {len(pct_top)} BY PROBABILITY MOVE (7D)\n")
    for r, m in enumerate(pct_top, 1):
        change = m["price_change_7d"]
        arrow = "▲" if change > 0 else "▼"
        print(
            f"  {r:>2}. {arrow}{abs(change)*100:>5.1f}pp  "
            f"now {m['yes_price']*100:.1f}%  "
            f"vol ${m['volume_24h']:,.0f}  "
            f"{m['question'][:50]}"
        )
    print()

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket scanner")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    raw_markets = fetch_active_markets()
    if not raw_markets:
        print("No markets returned. Exiting.", file=sys.stderr)
        sys.exit(1)

    markets = [p for m in raw_markets if (p := parse_market(m)) is not None]
    print(f"Parsed {len(markets)} markets successfully")

    vol_top = top_by_volume(markets, TOP_N)
    pct_top = top_by_pct_change(markets, TOP_N)

    scan_id = save_scan(conn, markets, vol_top, pct_top)
    conn.close()
    print(f"Saved scan #{scan_id} to {DB_PATH}")

    print_report(vol_top, pct_top)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done ✓")

if __name__ == "__main__":
    main()
