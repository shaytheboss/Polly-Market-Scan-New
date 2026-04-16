"""
Polymarket Weather Scanner v5
==============================
FAST: only fetches winner trades for the top N markets by volume.
Skips tiny markets entirely — the interesting traders are in big markets.
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
TOP_N_WINNERS          = int(os.getenv("TOP_N_WINNERS", "100"))
MIN_TRADE_USDC         = float(os.getenv("MIN_TRADE_USDC", "5"))
MIN_PROFIT_USDC        = float(os.getenv("MIN_PROFIT_USDC", "2"))
MIN_MARKET_VOLUME      = float(os.getenv("MIN_MARKET_VOLUME", "500"))   # skip markets below this
MAX_MARKETS_FOR_WINNERS = int(os.getenv("MAX_MARKETS_FOR_WINNERS", "30")) # hard cap
RESOLVED_LOOKBACK_DAYS = int(os.getenv("RESOLVED_LOOKBACK_DAYS", "3"))

# ─── Weather filter ───────────────────────────────────────────────────────────

TEMP_SIGNALS = [
    "temperature", "highest temperature", "lowest temperature",
    "high temp", "low temp", "°c", "°f", "celsius", "fahrenheit",
    "heat wave", "heatwave", "hottest", "coldest", "warmest",
    "record high", "record low", "rainfall", "precipitation",
    "snowfall", "snow accumulation", "hurricane season", "tropical storm",
]

FALSE_POSITIVES = [
    "hurricanes vs", "vs hurricanes", "miami heat", "heat vs", "vs heat",
    "thunder vs", "vs thunder", "rainbow six", "counter-strike", "csgo",
    "ipl", "super kings", "zalmi", "gladiators", "spread:", "handicap",
    "o/u", "game 1", "game 2", "bo3", "bo5", "iran", "military",
    "strike", "ceasefire", "cold war", "valve remove", "map pool",
]

def is_weather(question: str) -> bool:
    q = question.lower()
    if not any(s in q for s in TEMP_SIGNALS):
        return False
    if any(f in q for f in FALSE_POSITIVES):
        return False
    return True

def get_winning_outcome_index(m: dict) -> Optional[int]:
    op = m.get("outcomePrices")
    if isinstance(op, str):
        try:
            op = json.loads(op)
        except Exception:
            return None
    if not isinstance(op, list) or len(op) < 2:
        return None
    try:
        for i, p in enumerate([float(x) for x in op]):
            if p >= 0.99:
                return i
    except (ValueError, TypeError):
        pass
    return None

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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER REFERENCES daily_scans(id),
            rank INTEGER, market_id TEXT, condition_id TEXT,
            question TEXT, slug TEXT, volume_24h REAL, volume_total REAL,
            yes_price REAL, end_date TEXT, url TEXT
        );

        CREATE TABLE IF NOT EXISTS top_mover_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER REFERENCES daily_scans(id),
            rank INTEGER, market_id TEXT, condition_id TEXT,
            question TEXT, slug TEXT, yes_price_now REAL,
            price_change_7d REAL, volume_24h REAL, end_date TEXT, url TEXT
        );

        CREATE TABLE IF NOT EXISTS winners (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id                INTEGER REFERENCES daily_scans(id),
            scan_date              TEXT,
            proxy_wallet           TEXT,
            pseudonym              TEXT,
            market_question        TEXT,
            winning_side           TEXT,
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

        CREATE INDEX IF NOT EXISTS idx_scan_date     ON daily_scans(scan_date);
        CREATE INDEX IF NOT EXISTS idx_winner_wallet ON winners(proxy_wallet);
        CREATE INDEX IF NOT EXISTS idx_winner_scan   ON winners(scan_id);
        CREATE INDEX IF NOT EXISTS idx_winner_date   ON winners(scan_date);
    """)
    conn.commit()

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 45, retries: int = 3) -> list | dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "polymarket-weather/5.0"})
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

# ─── Market fetching ──────────────────────────────────────────────────────────

def fetch_active_weather() -> list[dict]:
    print("Fetching active markets...")
    raw = fetch_paginated(f"{GAMMA_API}/markets?active=true&closed=false")
    filtered = [m for m in raw if is_weather(m.get("question") or "")]
    print(f"  {len(raw)} total → {len(filtered)} weather markets")
    return filtered

def fetch_resolved_weather() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=RESOLVED_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Fetching resolved markets since {since}...")
    raw = fetch_paginated(
        f"{GAMMA_API}/markets?closed=true&after={since}&order=updatedAt&ascending=false"
    )
    filtered = [
        m for m in raw
        if is_weather(m.get("question") or "") and get_winning_outcome_index(m) is not None
    ]
    print(f"  {len(raw)} closed → {len(filtered)} resolved weather markets")
    return filtered

def parse_market(m: dict) -> Optional[dict]:
    try:
        op = m.get("outcomePrices")
        if isinstance(op, str):
            try: op = json.loads(op)
            except: op = []
        yes_price = None
        if isinstance(op, list) and len(op) >= 1:
            try: yes_price = float(op[0])
            except: pass
        if yes_price is None:
            yes_price = float(m.get("lastTradePrice") or 0)
        slug = m.get("slug") or ""
        return {
            "market_id":       str(m.get("id") or ""),
            "condition_id":    m.get("conditionId") or "",
            "question":        m.get("question") or "",
            "slug":            slug,
            "volume_24h":      float(m.get("volume24hr") or 0),
            "volume_total":    float(m.get("volume") or 0),
            "yes_price":       yes_price,
            "price_change_7d": float(m.get("oneWeekPriceChange") or 0),
            "end_date":        m.get("endDateIso") or m.get("endDate") or "",
            "url":             f"https://polymarket.com/event/{slug}" if slug else "",
        }
    except Exception as e:
        print(f"  Parse error {m.get('id','?')}: {e}", file=sys.stderr)
        return None

# ─── Winners — fast, top markets only ────────────────────────────────────────

def fetch_winners(resolved_markets: list[dict], top_n: int) -> list[dict]:
    # Filter by minimum volume and cap at MAX_MARKETS_FOR_WINNERS
    candidates = [
        m for m in resolved_markets
        if float(m.get("volume") or 0) >= MIN_MARKET_VOLUME
    ]
    # Sort by volume descending, take top N
    candidates = sorted(candidates, key=lambda m: float(m.get("volume") or 0), reverse=True)
    candidates = candidates[:MAX_MARKETS_FOR_WINNERS]

    skipped = len(resolved_markets) - len(candidates)
    print(f"\nFetching winners: {len(candidates)} markets")
    print(f"  (skipped {skipped} with volume < ${MIN_MARKET_VOLUME:,.0f} or beyond top {MAX_MARKETS_FOR_WINNERS})")

    all_winners: list[dict] = []

    for i, m in enumerate(candidates):
        cid           = m.get("conditionId") or ""
        question      = m.get("question") or ""
        slug          = m.get("slug") or ""
        market_url    = f"https://polymarket.com/event/{slug}" if slug else ""
        winning_index = get_winning_outcome_index(m)
        winning_side  = "YES" if winning_index == 0 else "NO"
        vol           = float(m.get("volume") or 0)

        if not cid or winning_index is None:
            continue

        print(f"  [{i+1}/{len(candidates)}] [{winning_side}✓] ${vol:,.0f}  {question[:55]}...")

        try:
            trades = fetch_json(
                f"{DATA_API}/trades?conditionId={cid}&side=BUY&outcomeIndex={winning_index}&limit=500"
            )
        except Exception as e:
            print(f"    Error: {e}", file=sys.stderr)
            continue

        if not trades:
            continue

        # Aggregate per wallet
        wallet_data: dict[str, dict] = {}
        for t in trades:
            wallet = t.get("proxyWallet") or ""
            if not wallet:
                continue
            usdc  = float(t.get("usdcSize") or 0)
            size  = float(t.get("size") or 0)
            price = float(t.get("price") or 0)
            ts    = int(t.get("timestamp") or 0)
            if usdc < MIN_TRADE_USDC:
                continue
            if wallet not in wallet_data:
                wallet_data[wallet] = {
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
            profit = d["tokens_bought"] - d["usdc_spent"]
            if profit < MIN_PROFIT_USDC:
                continue
            ts_iso = ""
            if d["earliest_ts"]:
                try:
                    ts_iso = datetime.fromtimestamp(d["earliest_ts"], tz=timezone.utc).isoformat()
                except Exception:
                    pass
            all_winners.append({
                "proxy_wallet":           wallet,
                "pseudonym":              d["pseudonym"],
                "market_question":        question,
                "winning_side":           winning_side,
                "condition_id":           cid,
                "market_slug":            slug,
                "market_url":             market_url,
                "trade_timestamp":        ts_iso,
                "entry_price":            d["earliest_price"],
                "usdc_spent":             round(d["usdc_spent"], 2),
                "tokens_bought":          round(d["tokens_bought"], 4),
                "profit_usdc":            round(profit, 2),
                "profit_pct":             round((profit / d["usdc_spent"]) * 100, 2),
                "polymarket_profile_url": f"https://polymarket.com/profile/{wallet}",
            })

    all_winners.sort(key=lambda w: w["profit_usdc"], reverse=True)
    return all_winners[:top_n]

# ─── DB writes ────────────────────────────────────────────────────────────────

def save_scan(conn, active, vol_top, mover_top, winners):
    now       = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y-%m-%d")
    cur       = conn.cursor()
    cur.execute(
        "INSERT INTO daily_scans (scan_date, scanned_at, markets_total, weather_total) VALUES (?,?,?,?)",
        (scan_date, now.isoformat(), len(active), len(active))
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

    for w in winners:
        cur.execute("""
            INSERT INTO winners
              (scan_id, scan_date, proxy_wallet, pseudonym, market_question,
               winning_side, condition_id, market_slug, market_url,
               trade_timestamp, entry_price, usdc_spent, tokens_bought,
               profit_usdc, profit_pct, polymarket_profile_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (scan_id, scan_date,
              w["proxy_wallet"], w["pseudonym"], w["market_question"],
              w["winning_side"], w["condition_id"], w["market_slug"],
              w["market_url"], w["trade_timestamp"], w["entry_price"],
              w["usdc_spent"], w["tokens_bought"],
              w["profit_usdc"], w["profit_pct"],
              w["polymarket_profile_url"]))

    conn.commit()
    return scan_id

# ─── Report ───────────────────────────────────────────────────────────────────

def print_report(vol_top, mover_top, winners):
    print("\n" + "="*72)
    print(f"  POLYMARKET WEATHER SCAN — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*72)

    print(f"\n🌡️  TOP {len(vol_top)} WEATHER MARKETS BY 24H VOLUME\n")
    for r, m in enumerate(vol_top, 1):
        p = f"{m['yes_price']*100:.1f}%" if m["yes_price"] else "N/A"
        print(f"  {r:>2}. ${m['volume_24h']:>10,.0f}  [{p}]  {m['question'][:62]}")
        print(f"       {m['url']}")

    print(f"\n📈 TOP {len(mover_top)} WEATHER MARKETS BY PROBABILITY MOVE (7D)\n")
    for r, m in enumerate(mover_top, 1):
        c = m["price_change_7d"]
        print(f"  {r:>2}. {'▲' if c>0 else '▼'}{abs(c)*100:>5.1f}pp  now {m['yes_price']*100:.1f}%  vol ${m['volume_24h']:,.0f}")
        print(f"       {m['question'][:65]}")
        print(f"       {m['url']}")

    print(f"\n🎯 TOP {len(winners)} WINNING TRADERS (last {RESOLVED_LOOKBACK_DAYS}d)\n")
    if not winners:
        print("  No winners found.\n")
        return
    for r, w in enumerate(winners, 1):
        name  = w["pseudonym"] or w["proxy_wallet"][:12] + "..."
        entry = f"{w['entry_price']*100:.1f}%" if w["entry_price"] else "?"
        ts    = w["trade_timestamp"][:16].replace("T", " ") if w["trade_timestamp"] else "?"
        print(f"  {r:>2}. {name}  [{w['winning_side']} won]")
        print(f"       Profit:  +${w['profit_usdc']:,.2f}  ({w['profit_pct']:+.1f}%)")
        print(f"       Bet:     ${w['usdc_spent']:,.2f} @ {entry}  on {ts} UTC")
        print(f"       Market:  {w['market_question'][:65]}")
        print(f"       Profile: {w['polymarket_profile_url']}")
        print()

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket weather scanner v5")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    active_raw = fetch_active_weather()
    active     = [p for m in active_raw if (p := parse_market(m)) is not None]
    vol_top    = sorted([m for m in active if m["volume_24h"] > 0],
                        key=lambda m: m["volume_24h"], reverse=True)[:TOP_N_VOLUME]
    mover_top  = sorted([m for m in active if m["price_change_7d"] != 0],
                        key=lambda m: abs(m["price_change_7d"]), reverse=True)[:TOP_N_MOVERS]

    resolved_raw = fetch_resolved_weather()
    winners      = fetch_winners(resolved_raw, TOP_N_WINNERS)
    print(f"Found {len(winners)} winners")

    scan_id = save_scan(conn, active, vol_top, mover_top, winners)
    conn.close()
    print(f"Saved scan #{scan_id} to {DB_PATH}")

    print_report(vol_top, mover_top, winners)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done ✓")

if __name__ == "__main__":
    main()
