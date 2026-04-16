"""
Polymarket Weather Scanner
==========================
Scans only weather and temperature markets.
Finds top volume, top movers, and winning traders in this niche.
"""

import sqlite3
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import urllib.request
import urllib.error

# ─── Config ───────────────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"

DB_PATH                = os.getenv("DB_PATH", "polymarket.db")
TOP_N_VOLUME           = int(os.getenv("TOP_N_VOLUME", "50"))
TOP_N_MOVERS           = int(os.getenv("TOP_N_MOVERS", "50"))
TOP_N_WINNERS          = int(os.getenv("TOP_N_WINNERS", "50"))
MIN_TRADE_USDC         = float(os.getenv("MIN_TRADE_USDC", "10"))
RESOLVED_LOOKBACK_DAYS = int(os.getenv("RESOLVED_LOOKBACK_DAYS", "3"))

# Keywords that identify weather/temperature markets
WEATHER_KEYWORDS = [
    "temperature", "celsius", "fahrenheit", "degrees",
    "weather", "rainfall", "precipitation", "rain",
    "snow", "snowfall", "blizzard",
    "hurricane", "typhoon", "cyclone", "tropical storm",
    "heat wave", "heatwave", "cold snap",
    "frost", "freeze", "frozen",
    "drought", "flood", "flooding",
    "wind", "windspeed", "tornado",
    "hottest", "coldest", "warmest",
    "high temp", "low temp", "record high", "record low",
    "above average", "below average",
    # common city/region + temp combos
    "°c", "°f",
]

# Keywords that should EXCLUDE a market even if it matched above
# (e.g. "cold war" is not a weather market)
EXCLUDE_KEYWORDS = [
    "cold war", "cold case", "cold open",
    "wind down", "wind up",
    "flood of", "flooded with",
    "hot take", "hot mic",
    "frozen conflict", "freeze on",
]

def is_weather_market(question: str, description: str = "") -> bool:
    text = (question + " " + description).lower()
    if any(ex in text for ex in EXCLUDE_KEYWORDS):
        return False
    return any(kw in text for kw in WEATHER_KEYWORDS)

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,
            scanned_at    TEXT NOT NULL,
            markets_total INTEGER,
            weather_total INTEGER
        );

        CREATE TABLE IF NOT EXISTS top_volume_markets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id      INTEGER REFERENCES daily_scans(id),
            rank         INTEGER,
            market_id    TEXT,
            condition_id TEXT,
            question     TEXT,
            slug         TEXT,
            volume_24h   REAL,
            volume_total REAL,
            yes_price    REAL,
            end_date     TEXT,
            url          TEXT
        );

        CREATE TABLE IF NOT EXISTS top_mover_markets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id         INTEGER REFERENCES daily_scans(id),
            rank            INTEGER,
            market_id       TEXT,
            condition_id    TEXT,
            question        TEXT,
            slug            TEXT,
            yes_price_now   REAL,
            price_change_7d REAL,
            volume_24h      REAL,
            end_date        TEXT,
            url             TEXT
        );

        CREATE TABLE IF NOT EXISTS winners (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id                INTEGER REFERENCES daily_scans(id),
            rank                   INTEGER,
            proxy_wallet           TEXT,
            pseudonym              TEXT,
            market_question        TEXT,
            condition_id           TEXT,
            market_slug            TEXT,
            market_url             TEXT,
            trade_timestamp        TEXT,
            entry_price            REAL,
            usdc_spent             REAL,
            tokens_bought          REAL,
            profit_usdc            REAL,
            profit_pct             REAL,
            polymarket_profile_url TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scan_date    ON daily_scans(scan_date);
        CREATE INDEX IF NOT EXISTS idx_vol_scan     ON top_volume_markets(scan_id);
        CREATE INDEX IF NOT EXISTS idx_mover_scan   ON top_mover_markets(scan_id);
        CREATE INDEX IF NOT EXISTS idx_winner_scan  ON winners(scan_id);
    """)
    conn.commit()

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 30, retries: int = 3) -> list | dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "polymarket-weather-scanner/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt)
            elif e.code == 404:
                return []
            else:
                raise
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1)
    return []

def fetch_paginated(base_url: str, limit: int = 100, max_pages: int = 999) -> list[dict]:
    results = []
    offset = 0
    for _ in range(max_pages):
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}limit={limit}&offset={offset}"
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"  Warning at offset {offset}: {e}", file=sys.stderr)
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

# ─── Market fetching & filtering ──────────────────────────────────────────────

def fetch_and_filter_active() -> list[dict]:
    print("Fetching all active markets...")
    raw = fetch_paginated(f"{GAMMA_API}/markets?active=true&closed=false")
    print(f"  Total active: {len(raw)}")
    filtered = [m for m in raw if is_weather_market(
        m.get("question") or "",
        m.get("description") or ""
    )]
    print(f"  Weather markets: {len(filtered)}")
    return filtered

def fetch_and_filter_resolved() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=RESOLVED_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Fetching markets resolved since {since}...")
    raw = fetch_paginated(
        f"{GAMMA_API}/markets?closed=true&after={since}&order=updatedAt&ascending=false"
    )
    filtered = [
        m for m in raw
        if is_weather_market(m.get("question") or "", m.get("description") or "")
        and _resolved_yes(m)
    ]
    print(f"  Total closed: {len(raw)} → weather+resolved YES: {len(filtered)}")
    return filtered

def _resolved_yes(m: dict) -> bool:
    op = m.get("outcomePrices")
    if isinstance(op, str):
        try:
            op = json.loads(op)
        except Exception:
            return False
    if isinstance(op, list) and len(op) >= 1:
        try:
            return float(op[0]) >= 0.99
        except (ValueError, TypeError):
            pass
    return False

def parse_market(m: dict) -> Optional[dict]:
    try:
        vol_24h         = float(m.get("volume24hr") or 0)
        vol_total       = float(m.get("volume") or 0)
        price_change_7d = float(m.get("oneWeekPriceChange") or 0)

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

        slug         = m.get("slug") or ""
        condition_id = m.get("conditionId") or ""
        return {
            "market_id":       str(m.get("id") or ""),
            "condition_id":    condition_id,
            "question":        m.get("question") or "",
            "slug":            slug,
            "volume_24h":      vol_24h,
            "volume_total":    vol_total,
            "yes_price":       yes_price,
            "price_change_7d": price_change_7d,
            "end_date":        m.get("endDateIso") or m.get("endDate") or "",
            "url":             f"https://polymarket.com/event/{slug}" if slug else "",
        }
    except Exception as e:
        print(f"  Parse error {m.get('id','?')}: {e}", file=sys.stderr)
        return None

# ─── Rankings ─────────────────────────────────────────────────────────────────

def top_by_volume(markets, n):
    return sorted([m for m in markets if m["volume_24h"] > 0],
                  key=lambda m: m["volume_24h"], reverse=True)[:n]

def top_by_pct_change(markets, n):
    return sorted([m for m in markets if m["price_change_7d"] != 0],
                  key=lambda m: abs(m["price_change_7d"]), reverse=True)[:n]

# ─── Winners ──────────────────────────────────────────────────────────────────

def fetch_winners(resolved_markets: list[dict], top_n: int) -> list[dict]:
    all_winners = []
    print(f"\nFetching winning trades for {len(resolved_markets)} resolved weather markets...")

    for i, m in enumerate(resolved_markets):
        condition_id = m.get("conditionId") or m.get("condition_id") or ""
        question     = m.get("question") or ""
        slug         = m.get("slug") or ""
        market_url   = f"https://polymarket.com/event/{slug}" if slug else ""

        if not condition_id:
            continue

        print(f"  [{i+1}/{len(resolved_markets)}] {question[:60]}...")

        trades = fetch_paginated(
            f"{DATA_API}/trades?conditionId={condition_id}&side=BUY&outcomeIndex=0",
            limit=500, max_pages=5
        )
        if not trades:
            time.sleep(0.2)
            continue

        wallet_data: dict[str, dict] = {}
        for t in trades:
            wallet = t.get("proxyWallet") or ""
            if not wallet:
                continue
            usdc  = float(t.get("usdcSize") or 0)
            size  = float(t.get("size") or 0)
            price = float(t.get("price") or 0)
            ts    = t.get("timestamp") or 0

            if usdc < MIN_TRADE_USDC:
                continue

            if wallet not in wallet_data:
                wallet_data[wallet] = {
                    "proxy_wallet":   wallet,
                    "pseudonym":      t.get("pseudonym") or t.get("name") or "",
                    "usdc_spent":     0.0,
                    "tokens_bought":  0.0,
                    "earliest_ts":    ts,
                    "earliest_price": price,
                }

            wallet_data[wallet]["usdc_spent"]    += usdc
            wallet_data[wallet]["tokens_bought"] += size

            if ts and ts < wallet_data[wallet]["earliest_ts"]:
                wallet_data[wallet]["earliest_ts"]    = ts
                wallet_data[wallet]["earliest_price"] = price

        for wallet, d in wallet_data.items():
            if d["usdc_spent"] <= 0:
                continue
            profit_usdc = d["tokens_bought"] - d["usdc_spent"]
            profit_pct  = (profit_usdc / d["usdc_spent"]) * 100
            if profit_usdc <= 0:
                continue

            ts_iso = ""
            if d["earliest_ts"]:
                try:
                    ts_iso = datetime.fromtimestamp(d["earliest_ts"], tz=timezone.utc).isoformat()
                except Exception:
                    pass

            all_winners.append({
                "proxy_wallet":            wallet,
                "pseudonym":               d["pseudonym"],
                "market_question":         question,
                "condition_id":            condition_id,
                "market_slug":             slug,
                "market_url":              market_url,
                "trade_timestamp":         ts_iso,
                "entry_price":             d["earliest_price"],
                "usdc_spent":              round(d["usdc_spent"], 2),
                "tokens_bought":           round(d["tokens_bought"], 4),
                "profit_usdc":             round(profit_usdc, 2),
                "profit_pct":              round(profit_pct, 2),
                "polymarket_profile_url":  f"https://polymarket.com/profile/{wallet}",
            })

        time.sleep(0.3)

    all_winners.sort(key=lambda w: w["profit_usdc"], reverse=True)
    return all_winners[:top_n]

# ─── DB writes ────────────────────────────────────────────────────────────────

def save_scan(conn, all_markets, vol_top, mover_top, winners):
    now = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_scans (scan_date, scanned_at, markets_total, weather_total) VALUES (?,?,?,?)",
        (now.strftime("%Y-%m-%d"), now.isoformat(), len(all_markets), len(all_markets))
    )
    scan_id = cur.lastrowid

    for rank, m in enumerate(vol_top, 1):
        cur.execute("""
            INSERT INTO top_volume_markets
              (scan_id, rank, market_id, condition_id, question, slug,
               volume_24h, volume_total, yes_price, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, rank, m["market_id"], m["condition_id"], m["question"],
              m["slug"], m["volume_24h"], m["volume_total"],
              m["yes_price"], m["end_date"], m["url"]))

    for rank, m in enumerate(mover_top, 1):
        cur.execute("""
            INSERT INTO top_mover_markets
              (scan_id, rank, market_id, condition_id, question, slug,
               yes_price_now, price_change_7d, volume_24h, end_date, url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, rank, m["market_id"], m["condition_id"], m["question"],
              m["slug"], m["yes_price"], m["price_change_7d"],
              m["volume_24h"], m["end_date"], m["url"]))

    for rank, w in enumerate(winners, 1):
        cur.execute("""
            INSERT INTO winners
              (scan_id, rank, proxy_wallet, pseudonym, market_question,
               condition_id, market_slug, market_url, trade_timestamp,
               entry_price, usdc_spent, tokens_bought, profit_usdc,
               profit_pct, polymarket_profile_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, rank,
              w["proxy_wallet"], w["pseudonym"], w["market_question"],
              w["condition_id"], w["market_slug"], w["market_url"],
              w["trade_timestamp"], w["entry_price"],
              w["usdc_spent"], w["tokens_bought"],
              w["profit_usdc"], w["profit_pct"],
              w["polymarket_profile_url"]))

    conn.commit()
    return scan_id

# ─── Report ───────────────────────────────────────────────────────────────────

def print_report(vol_top, mover_top, winners):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("\n" + "="*72)
    print(f"  POLYMARKET WEATHER SCAN — {now_str}")
    print("="*72)

    print(f"\n🌡️  TOP {len(vol_top)} WEATHER MARKETS BY 24H VOLUME\n")
    if not vol_top:
        print("  No weather markets with volume found.")
    for r, m in enumerate(vol_top, 1):
        p = f"{m['yes_price']*100:.1f}%" if m["yes_price"] else "N/A"
        print(f"  {r:>2}. ${m['volume_24h']:>10,.0f}  [{p}]  {m['question'][:62]}")
        print(f"       {m['url']}")

    print(f"\n📈 TOP {len(mover_top)} WEATHER MARKETS BY PROBABILITY MOVE (7D)\n")
    if not mover_top:
        print("  No movers found.")
    for r, m in enumerate(mover_top, 1):
        c = m["price_change_7d"]
        arrow = "▲" if c > 0 else "▼"
        print(
            f"  {r:>2}. {arrow}{abs(c)*100:>5.1f}pp  "
            f"now {m['yes_price']*100:.1f}%  "
            f"vol ${m['volume_24h']:,.0f}  "
            f"{m['question'][:50]}"
        )
        print(f"       {m['url']}")

    print(f"\n🎯 TOP {len(winners)} WINNING TRADERS (weather markets, last {RESOLVED_LOOKBACK_DAYS}d)\n")
    if not winners:
        print("  No winners found for this period.\n")
        return
    for r, w in enumerate(winners, 1):
        name    = w["pseudonym"] or w["proxy_wallet"][:10] + "..."
        entry_p = f"{w['entry_price']*100:.1f}%" if w["entry_price"] else "?"
        ts      = w["trade_timestamp"][:16].replace("T", " ") if w["trade_timestamp"] else "?"
        print(f"  {r:>2}. {name}")
        print(f"       Profit:  +${w['profit_usdc']:,.2f}  ({w['profit_pct']:+.1f}%)")
        print(f"       Bet:     ${w['usdc_spent']:,.2f} @ {entry_p}  placed {ts} UTC")
        print(f"       Market:  {w['market_question'][:65]}")
        print(f"       Profile: {w['polymarket_profile_url']}")
        print()

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket weather scanner")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Active weather markets
    active_raw    = fetch_and_filter_active()
    active        = [p for m in active_raw if (p := parse_market(m)) is not None]

    vol_top   = top_by_volume(active, TOP_N_VOLUME)
    mover_top = top_by_pct_change(active, TOP_N_MOVERS)

    # Resolved weather markets → winners
    resolved_raw = fetch_and_filter_resolved()
    winners      = fetch_winners(resolved_raw, TOP_N_WINNERS)

    scan_id = save_scan(conn, active, vol_top, mover_top, winners)
    conn.close()
    print(f"\nSaved scan #{scan_id} to {DB_PATH}")

    print_report(vol_top, mover_top, winners)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done ✓")

if __name__ == "__main__":
    main()
