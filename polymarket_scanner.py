"""
Polymarket Daily Scanner
========================
Scans for:
  1. Top markets by 24h USD volume
  2. Top markets by 7d probability move
  3. WINNERS — traders who bought YES on markets that resolved YES today,
     including their username (pseudonym), entry price, entry time, and profit.

APIs used (all public, no auth needed):
  Gamma API   — https://gamma-api.polymarket.com   (market metadata)
  Data API    — https://data-api.polymarket.com     (trades, holders, positions)
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

GAMMA_API   = "https://gamma-api.polymarket.com"
DATA_API    = "https://data-api.polymarket.com"

DB_PATH          = os.getenv("DB_PATH", "polymarket.db")
TOP_N_VOLUME     = int(os.getenv("TOP_N_VOLUME", "50"))    # top markets by volume
TOP_N_MOVERS     = int(os.getenv("TOP_N_MOVERS", "50"))    # top markets by % move
TOP_N_WINNERS    = int(os.getenv("TOP_N_WINNERS", "50"))   # top winning traders
MIN_TRADE_USDC   = float(os.getenv("MIN_TRADE_USDC", "50"))  # ignore tiny trades
RESOLVED_LOOKBACK_DAYS = int(os.getenv("RESOLVED_LOOKBACK_DAYS", "1"))  # how many days back to look for resolved markets

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,
            scanned_at    TEXT NOT NULL,
            markets_total INTEGER
        );

        -- Top markets by 24h trading volume
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

        -- Top markets by 7d probability move
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

        -- Winning traders: people who bought YES on markets that just resolved YES
        CREATE TABLE IF NOT EXISTS winners (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id           INTEGER REFERENCES daily_scans(id),
            rank              INTEGER,
            proxy_wallet      TEXT,          -- blockchain address
            pseudonym         TEXT,          -- display name on Polymarket
            market_question   TEXT,
            condition_id      TEXT,
            market_slug       TEXT,
            market_url        TEXT,
            trade_timestamp   TEXT,          -- ISO datetime when they placed the bet
            entry_price       REAL,          -- probability when they bought (e.g. 0.35 = 35%)
            usdc_spent        REAL,          -- how much they bet in USD
            tokens_bought     REAL,          -- number of YES tokens bought
            profit_usdc       REAL,          -- approx profit: tokens_bought - usdc_spent
            profit_pct        REAL,          -- profit % relative to spend
            polymarket_profile_url TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scan_date    ON daily_scans(scan_date);
        CREATE INDEX IF NOT EXISTS idx_vol_scan     ON top_volume_markets(scan_id);
        CREATE INDEX IF NOT EXISTS idx_mover_scan   ON top_mover_markets(scan_id);
        CREATE INDEX IF NOT EXISTS idx_winner_scan  ON winners(scan_id);
        CREATE INDEX IF NOT EXISTS idx_winner_wallet ON winners(proxy_wallet);
    """)
    conn.commit()

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch_json(url: str, timeout: int = 30, retries: int = 3) -> dict | list:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "polymarket-scanner/2.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
            elif e.code == 404:
                return []
            else:
                raise
        except Exception as e:
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

# ─── Market data ──────────────────────────────────────────────────────────────

def fetch_active_markets() -> list[dict]:
    print("Fetching active markets...")
    markets = fetch_paginated(f"{GAMMA_API}/markets?active=true&closed=false")
    print(f"  Got {len(markets)} active markets")
    return markets

def fetch_recently_resolved_markets() -> list[dict]:
    """Fetch markets that resolved in the last N days."""
    since = datetime.now(timezone.utc) - timedelta(days=RESOLVED_LOOKBACK_DAYS)
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Fetching markets resolved since {since_iso}...")
    markets = fetch_paginated(
        f"{GAMMA_API}/markets?closed=true&after={since_iso}&order=updatedAt&ascending=false"
    )
    # Filter to only truly resolved ones (outcomePrices has a winner at ~1.0)
    resolved = [m for m in markets if _is_resolved_yes(m)]
    print(f"  Got {len(markets)} recently closed, {len(resolved)} resolved YES")
    return resolved

def _is_resolved_yes(m: dict) -> bool:
    """Returns True if the YES outcome resolved to 1.0 (winner)."""
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
        vol_24h   = float(m.get("volume24hr") or 0)
        vol_total = float(m.get("volume") or 0)
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

        # conditionId is what the Data API uses for trades/holders
        condition_id = m.get("conditionId") or ""
        market_id    = str(m.get("id") or "")
        slug         = m.get("slug") or ""
        question     = m.get("question") or ""
        end_date     = m.get("endDateIso") or m.get("endDate") or ""
        url          = f"https://polymarket.com/event/{slug}" if slug else ""

        return {
            "market_id":       market_id,
            "condition_id":    condition_id,
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
        print(f"  Parse error market {m.get('id','?')}: {e}", file=sys.stderr)
        return None

# ─── Rankings ─────────────────────────────────────────────────────────────────

def top_by_volume(markets: list[dict], n: int) -> list[dict]:
    valid = [m for m in markets if m["volume_24h"] > 0]
    return sorted(valid, key=lambda m: m["volume_24h"], reverse=True)[:n]

def top_by_pct_change(markets: list[dict], n: int) -> list[dict]:
    valid = [m for m in markets if m["volume_24h"] > 1000 and m["price_change_7d"] != 0]
    return sorted(valid, key=lambda m: abs(m["price_change_7d"]), reverse=True)[:n]

# ─── Winners ──────────────────────────────────────────────────────────────────

def fetch_winners(resolved_markets: list[dict], top_n: int) -> list[dict]:
    """
    For each resolved-YES market, fetch the trades from buyers who profited.
    Returns a flat list of winner records sorted by profit.
    """
    all_winners = []

    print(f"\nFetching winning trades for {len(resolved_markets)} resolved markets...")

    for i, m in enumerate(resolved_markets):
        condition_id = m.get("conditionId") or m.get("condition_id") or ""
        question     = m.get("question") or ""
        slug         = m.get("slug") or ""
        market_url   = f"https://polymarket.com/event/{slug}" if slug else ""

        if not condition_id:
            continue

        print(f"  [{i+1}/{len(resolved_markets)}] {question[:55]}...")

        # Fetch all BUY trades for YES outcome on this market
        # outcomeIndex=0 is YES, side=BUY
        trades = fetch_paginated(
            f"{DATA_API}/trades?conditionId={condition_id}&side=BUY&outcomeIndex=0",
            limit=500,
            max_pages=5  # cap at 2500 trades per market
        )

        if not trades:
            time.sleep(0.2)
            continue

        # Group trades by wallet — sum up what they spent and bought
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

            # Track earliest trade (original entry)
            if ts and ts < wallet_data[wallet]["earliest_ts"]:
                wallet_data[wallet]["earliest_ts"]    = ts
                wallet_data[wallet]["earliest_price"] = price

        # Each token pays out $1 on YES resolution → profit = tokens - cost
        for wallet, d in wallet_data.items():
            if d["usdc_spent"] <= 0:
                continue
            profit_usdc = d["tokens_bought"] - d["usdc_spent"]
            profit_pct  = (profit_usdc / d["usdc_spent"]) * 100

            if profit_usdc <= 0:
                continue

            # Convert unix timestamp to ISO string
            ts_iso = ""
            if d["earliest_ts"]:
                try:
                    ts_iso = datetime.fromtimestamp(
                        d["earliest_ts"], tz=timezone.utc
                    ).isoformat()
                except Exception:
                    pass

            all_winners.append({
                "proxy_wallet":    wallet,
                "pseudonym":       d["pseudonym"],
                "market_question": question,
                "condition_id":    condition_id,
                "market_slug":     slug,
                "market_url":      market_url,
                "trade_timestamp": ts_iso,
                "entry_price":     d["earliest_price"],
                "usdc_spent":      round(d["usdc_spent"], 2),
                "tokens_bought":   round(d["tokens_bought"], 4),
                "profit_usdc":     round(profit_usdc, 2),
                "profit_pct":      round(profit_pct, 2),
                "polymarket_profile_url": f"https://polymarket.com/profile/{wallet}",
            })

        time.sleep(0.3)  # be polite to the API

    # Sort by absolute profit
    all_winners.sort(key=lambda w: w["profit_usdc"], reverse=True)
    return all_winners[:top_n]

# ─── DB writes ────────────────────────────────────────────────────────────────

def save_scan(conn, markets, vol_top, mover_top, winners):
    now = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO daily_scans (scan_date, scanned_at, markets_total) VALUES (?,?,?)",
        (now.strftime("%Y-%m-%d"), now.isoformat(), len(markets))
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
    print(f"  POLYMARKET DAILY SCAN — {now_str}")
    print("="*72)

    print(f"\n🏆 TOP {len(vol_top)} MARKETS BY 24H VOLUME\n")
    for r, m in enumerate(vol_top, 1):
        p = f"{m['yes_price']*100:.1f}%" if m["yes_price"] else "N/A"
        print(f"  {r:>2}. ${m['volume_24h']:>12,.0f}  [{p}]  {m['question'][:62]}")

    print(f"\n📈 TOP {len(mover_top)} MARKETS BY PROBABILITY MOVE (7D)\n")
    for r, m in enumerate(mover_top, 1):
        c = m["price_change_7d"]
        arrow = "▲" if c > 0 else "▼"
        print(
            f"  {r:>2}. {arrow}{abs(c)*100:>5.1f}pp  "
            f"now {m['yes_price']*100:.1f}%  "
            f"vol ${m['volume_24h']:,.0f}  "
            f"{m['question'][:48]}"
        )

    print(f"\n🎯 TOP {len(winners)} WINNING TRADERS (resolved YES markets)\n")
    if not winners:
        print("  No winners found for this period.\n")
        return

    for r, w in enumerate(winners, 1):
        name    = w["pseudonym"] or w["proxy_wallet"][:10] + "..."
        entry_p = f"{w['entry_price']*100:.1f}%" if w["entry_price"] else "?"
        ts      = w["trade_timestamp"][:16].replace("T", " ") if w["trade_timestamp"] else "?"
        print(
            f"  {r:>2}. {name:<22}  "
            f"+${w['profit_usdc']:>9,.2f}  ({w['profit_pct']:+.1f}%)  "
            f"bet ${w['usdc_spent']:,.0f} @ {entry_p}  [{ts}]"
        )
        print(f"       Market: {w['market_question'][:58]}")
        print(f"       Profile: {w['polymarket_profile_url']}")
    print()

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket scanner v2")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Active markets → volume & movers rankings
    raw_active  = fetch_active_markets()
    active      = [p for m in raw_active if (p := parse_market(m)) is not None]
    print(f"Parsed {len(active)} active markets")

    vol_top   = top_by_volume(active, TOP_N_VOLUME)
    mover_top = top_by_pct_change(active, TOP_N_MOVERS)

    # Resolved markets → find winning traders
    resolved_raw = fetch_recently_resolved_markets()
    winners      = fetch_winners(resolved_raw, TOP_N_WINNERS)
    print(f"Found {len(winners)} winning traders")

    scan_id = save_scan(conn, active, vol_top, mover_top, winners)
    conn.close()
    print(f"Saved scan #{scan_id} to {DB_PATH}")

    print_report(vol_top, mover_top, winners)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done ✓")

if __name__ == "__main__":
    main()
