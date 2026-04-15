"""
Polymarket Daily Scanner
Finds top gainers by absolute $ volume and % probability change.
Saves results to SQLite database.
"""

import sqlite3
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional
import urllib.request
import urllib.error

# ─── Config ──────────────────────────────────────────────────────────────────

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE  = "https://clob.polymarket.com"

DB_PATH = os.getenv("DB_PATH", "polymarket.db")
TOP_N   = int(os.getenv("TOP_N", "20"))          # how many top movers to save

# ─── Database setup ───────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,          -- YYYY-MM-DD
            scanned_at    TEXT NOT NULL,           -- ISO timestamp
            markets_total INTEGER
        );

        CREATE TABLE IF NOT EXISTS top_gainers_volume (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id       INTEGER REFERENCES daily_scans(id),
            rank          INTEGER,
            market_id     TEXT,
            question      TEXT,
            slug          TEXT,
            volume_24h    REAL,                   -- USD volume last 24h
            volume_total  REAL,
            yes_price     REAL,                   -- current YES probability
            end_date      TEXT,
            url           TEXT
        );

        CREATE TABLE IF NOT EXISTS top_movers_pct (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id       INTEGER REFERENCES daily_scans(id),
            rank          INTEGER,
            market_id     TEXT,
            question      TEXT,
            slug          TEXT,
            yes_price_now  REAL,
            yes_price_prev REAL,
            price_change   REAL,                  -- absolute change in probability points
            pct_change     REAL,                  -- % change relative to previous price
            volume_24h     REAL,
            end_date       TEXT,
            url            TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scan_date  ON daily_scans(scan_date);
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
    """Fetch all pages from a paginated Gamma API endpoint."""
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

        # Gamma API returns list directly
        if isinstance(data, list):
            results.extend(data)
            if len(data) < limit:
                break
            offset += limit
        # Some endpoints wrap in 'data'
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
    """Fetch all active markets from Gamma API."""
    print("Fetching active markets from Gamma API...")
    url = f"{GAMMA_API_BASE}/markets?active=true&closed=false"
    markets = fetch_paginated(url)
    print(f"  Got {len(markets)} active markets")
    return markets

def fetch_market_history_24h(market_slug: str) -> Optional[dict]:
    """
    Fetch 24h price history for a single market from Gamma API.
    Returns dict with prev_yes_price or None on failure.
    """
    try:
        # Gamma API history endpoint
        url = f"{GAMMA_API_BASE}/markets/{market_slug}/history?interval=1d&fidelity=60"
        data = fetch_json(url)
        if isinstance(data, list) and len(data) >= 2:
            # history is list of {t: timestamp, p: price}
            return {"prev": data[0].get("p"), "current": data[-1].get("p")}
        elif isinstance(data, dict) and "history" in data:
            history = data["history"]
            if len(history) >= 2:
                return {"prev": history[0].get("p"), "current": history[-1].get("p")}
    except Exception:
        pass
    return None

# ─── Processing ───────────────────────────────────────────────────────────────

def parse_market(m: dict) -> Optional[dict]:
    """Extract the fields we care about from a raw market dict."""
    try:
        # Volume: Gamma uses volume24hr or volume
        vol_24h  = float(m.get("volume24hr") or m.get("volume24Hour") or 0)
        vol_total = float(m.get("volume") or 0)

        # Current YES probability
        # outcomePrices is a JSON string like '["0.65", "0.35"]' or list
        op = m.get("outcomePrices") or m.get("outcomes")
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

        # Fallback: lastTradePrice
        if yes_price is None:
            yes_price = float(m.get("lastTradePrice") or m.get("bestBid") or 0)

        market_id = str(m.get("id") or m.get("conditionId") or "")
        slug      = m.get("slug") or m.get("marketSlug") or ""
        question  = m.get("question") or m.get("title") or ""
        end_date  = m.get("endDate") or m.get("endDateIso") or ""
        url       = f"https://polymarket.com/event/{slug}" if slug else ""

        return {
            "market_id":    market_id,
            "question":     question,
            "slug":         slug,
            "volume_24h":   vol_24h,
            "volume_total": vol_total,
            "yes_price":    yes_price,
            "end_date":     end_date,
            "url":          url,
        }
    except Exception as e:
        print(f"  Parse error on market {m.get('id','?')}: {e}", file=sys.stderr)
        return None

def top_by_volume(markets: list[dict], n: int) -> list[dict]:
    """Return top N markets by 24h USD volume."""
    valid = [m for m in markets if m["volume_24h"] > 0]
    ranked = sorted(valid, key=lambda m: m["volume_24h"], reverse=True)
    return ranked[:n]

def top_by_pct_change(markets: list[dict], n: int) -> list[dict]:
    """
    Return top N markets by absolute % change in YES probability over 24h.
    Fetches history for each top-volume candidate to avoid hitting the API
    for every single market.
    """
    # Only check top 200 by volume to limit API calls
    candidates = sorted(markets, key=lambda m: m["volume_24h"], reverse=True)[:200]
    movers = []

    print(f"Fetching 24h price history for {len(candidates)} candidate markets...")
    for i, m in enumerate(candidates):
        if not m["slug"]:
            continue
        hist = fetch_market_history_24h(m["slug"])
        if hist and hist["prev"] is not None and hist["current"] is not None:
            prev = float(hist["prev"])
            curr = float(hist["current"])
            if prev > 0.001:  # avoid division by near-zero
                change_pts = curr - prev
                pct_change = (change_pts / prev) * 100
                movers.append({
                    **m,
                    "yes_price_now":  curr,
                    "yes_price_prev": prev,
                    "price_change":   round(change_pts, 4),
                    "pct_change":     round(pct_change, 2),
                })
        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{len(candidates)}...")

    # Sort by absolute % change (biggest move, up or down)
    ranked = sorted(movers, key=lambda m: abs(m["pct_change"]), reverse=True)
    return ranked[:n]

# ─── Database writes ──────────────────────────────────────────────────────────

def save_scan(conn: sqlite3.Connection, markets: list[dict], vol_top: list[dict], pct_top: list[dict]):
    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y-%m-%d")
    scanned_at = now.isoformat()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_scans (scan_date, scanned_at, markets_total) VALUES (?,?,?)",
        (scan_date, scanned_at, len(markets))
    )
    scan_id = cur.lastrowid

    # Top gainers by volume
    for rank, m in enumerate(vol_top, 1):
        cur.execute("""
            INSERT INTO top_gainers_volume
              (scan_id, rank, market_id, question, slug, volume_24h, volume_total,
               yes_price, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_id, rank,
            m["market_id"], m["question"], m["slug"],
            m["volume_24h"], m["volume_total"],
            m["yes_price"], m["end_date"], m["url"],
        ))

    # Top movers by % change
    for rank, m in enumerate(pct_top, 1):
        cur.execute("""
            INSERT INTO top_movers_pct
              (scan_id, rank, market_id, question, slug,
               yes_price_now, yes_price_prev, price_change, pct_change,
               volume_24h, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            scan_id, rank,
            m["market_id"], m["question"], m["slug"],
            m["yes_price_now"], m["yes_price_prev"],
            m["price_change"], m["pct_change"],
            m["volume_24h"], m["end_date"], m["url"],
        ))

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

    print(f"\n📈 TOP {len(pct_top)} BY PROBABILITY MOVE (24H)\n")
    for r, m in enumerate(pct_top, 1):
        arrow = "▲" if m["pct_change"] > 0 else "▼"
        print(
            f"  {r:>2}. {arrow}{abs(m['pct_change']):>6.1f}%  "
            f"({m['yes_price_prev']*100:.1f}% → {m['yes_price_now']*100:.1f}%)  "
            f"{m['question'][:55]}"
        )
    print()

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket scanner")

    # Init DB
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Fetch
    raw_markets = fetch_active_markets()
    if not raw_markets:
        print("No markets returned. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Parse
    markets = [p for m in raw_markets if (p := parse_market(m)) is not None]
    print(f"Parsed {len(markets)} markets successfully")

    # Rank
    vol_top = top_by_volume(markets, TOP_N)
    pct_top = top_by_pct_change(markets, TOP_N)

    # Save
    scan_id = save_scan(conn, markets, vol_top, pct_top)
    conn.close()
    print(f"Saved scan #{scan_id} to {DB_PATH}")

    # Print summary
    print_report(vol_top, pct_top)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done ✓")

if __name__ == "__main__":
    main()
