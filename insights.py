"""
Polymarket Weather — Trader Insights
======================================
Reads the DB built by polymarket_scanner.py and produces three rankings:

  1. CONSISTENCY  — who wins most often (win rate across markets)
  2. TOTAL PROFIT — who made the most money overall
  3. TIMING       — who enters earliest before the market moves

Usage:
  python insights.py                    # last 30 days
  python insights.py --days 7
  python insights.py --days 90
  python insights.py --top 20
  python insights.py --wallet 0xABC...  # deep dive on one trader
"""

import sqlite3
import sys
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

DB_PATH = "polymarket.db"

# ─── Load raw winner rows ─────────────────────────────────────────────────────

def load_winners(conn: sqlite3.Connection, days: int) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT proxy_wallet, pseudonym, market_question, winning_side,
               market_url, trade_timestamp, entry_price,
               usdc_spent, profit_usdc, profit_pct,
               polymarket_profile_url, scan_date
        FROM winners
        WHERE scan_date >= ?
        ORDER BY scan_date DESC
    """, (since,)).fetchall()

    cols = ["proxy_wallet", "pseudonym", "market_question", "winning_side",
            "market_url", "trade_timestamp", "entry_price",
            "usdc_spent", "profit_usdc", "profit_pct",
            "polymarket_profile_url", "scan_date"]
    return [dict(zip(cols, r)) for r in rows]

# ─── Aggregate per trader ─────────────────────────────────────────────────────

def aggregate_traders(winners: list[dict]) -> dict[str, dict]:
    traders: dict[str, dict] = {}

    for w in winners:
        wallet = w["proxy_wallet"]
        if wallet not in traders:
            traders[wallet] = {
                "proxy_wallet":   wallet,
                "pseudonym":      w["pseudonym"] or "",
                "profile_url":    w["polymarket_profile_url"],
                "wins":           0,
                "total_profit":   0.0,
                "total_spent":    0.0,
                "markets":        [],       # list of questions they won
                "entry_prices":   [],       # entry price per win
                "timestamps":     [],       # trade timestamps
                "sides":          [],       # YES or NO per win
            }

        t = traders[wallet]
        # Keep the most recent non-empty pseudonym
        if w["pseudonym"]:
            t["pseudonym"] = w["pseudonym"]

        t["wins"]         += 1
        t["total_profit"] += w["profit_usdc"]
        t["total_spent"]  += w["usdc_spent"]
        t["markets"].append(w["market_question"])
        t["entry_prices"].append(w["entry_price"] or 0)
        t["sides"].append(w["winning_side"])
        if w["trade_timestamp"]:
            t["timestamps"].append(w["trade_timestamp"])

    # Compute derived metrics
    for wallet, t in traders.items():
        t["avg_profit_per_win"] = t["total_profit"] / t["wins"] if t["wins"] else 0
        t["roi_pct"] = (t["total_profit"] / t["total_spent"] * 100) if t["total_spent"] > 0 else 0
        t["avg_entry_price"] = sum(t["entry_prices"]) / len(t["entry_prices"]) if t["entry_prices"] else 0
        # "early" = average entry price far from 1.0 (means they entered when odds were uncertain)
        # Lower avg entry = entered earlier / at longer odds
        t["avg_entry_pct"] = t["avg_entry_price"] * 100

    return traders

# ─── Rankings ─────────────────────────────────────────────────────────────────

def rank_by_consistency(traders: dict, top_n: int) -> list[dict]:
    """Most wins. Tiebreak: ROI%"""
    return sorted(traders.values(),
                  key=lambda t: (t["wins"], t["roi_pct"]),
                  reverse=True)[:top_n]

def rank_by_total_profit(traders: dict, top_n: int) -> list[dict]:
    """Most total USDC profit."""
    return sorted(traders.values(),
                  key=lambda t: t["total_profit"],
                  reverse=True)[:top_n]

def rank_by_timing(traders: dict, top_n: int) -> list[dict]:
    """
    Best timing = lowest average entry price on winning trades.
    If you bought YES at 20% and it resolved YES, you entered early.
    If you bought NO at 20% (= market priced YES at 80%) and NO won, also early.
    We normalize: for NO wins, early entry = high YES price (so NO was cheap).
    """
    def timing_score(t: dict) -> float:
        if not t["entry_prices"] or not t["sides"]:
            return 0.5
        scores = []
        for price, side in zip(t["entry_prices"], t["sides"]):
            if side == "YES":
                scores.append(price)        # low = early (cheap YES)
            else:
                scores.append(1.0 - price)  # low = early (cheap NO)
        return sum(scores) / len(scores)

    # Lower timing_score = entered earlier
    ranked = sorted(
        [t for t in traders.values() if t["wins"] >= 2],  # need at least 2 wins to be meaningful
        key=timing_score
    )
    for t in ranked:
        t["_timing_score"] = timing_score(t)
    return ranked[:top_n]

# ─── Display ──────────────────────────────────────────────────────────────────

def show_consistency(ranked: list[dict], days: int):
    print(f"\n🏆 MOST CONSISTENT WINNERS (last {days} days)\n")
    print(f"  {'#':<3} {'Name':<22} {'Wins':>5} {'Total $':>10} {'ROI':>7}  Profile")
    print("  " + "-"*70)
    for r, t in enumerate(ranked, 1):
        name = t["pseudonym"] or t["proxy_wallet"][:12] + "..."
        print(f"  {r:<3} {name:<22} {t['wins']:>5} "
              f"${t['total_profit']:>9,.2f} {t['roi_pct']:>6.1f}%  {t['profile_url']}")
    print()

def show_profit(ranked: list[dict], days: int):
    print(f"\n💰 HIGHEST TOTAL PROFIT (last {days} days)\n")
    print(f"  {'#':<3} {'Name':<22} {'Total $':>10} {'Wins':>5} {'Avg/win':>9}  Profile")
    print("  " + "-"*70)
    for r, t in enumerate(ranked, 1):
        name = t["pseudonym"] or t["proxy_wallet"][:12] + "..."
        print(f"  {r:<3} {name:<22} "
              f"${t['total_profit']:>9,.2f} {t['wins']:>5} "
              f"${t['avg_profit_per_win']:>8,.2f}  {t['profile_url']}")
    print()

def show_timing(ranked: list[dict], days: int):
    print(f"\n⏱️  BEST TIMING — entered earliest (last {days} days, min 2 wins)\n")
    print(f"  {'#':<3} {'Name':<22} {'Avg entry':>10} {'Wins':>5} {'Total $':>10}  Profile")
    print("  " + "-"*70)
    for r, t in enumerate(ranked, 1):
        name  = t["pseudonym"] or t["proxy_wallet"][:12] + "..."
        score = t.get("_timing_score", 0)
        print(f"  {r:<3} {name:<22} {score*100:>9.1f}% {t['wins']:>5} "
              f"${t['total_profit']:>9,.2f}  {t['profile_url']}")
    print(f"\n  Note: lower % = entered when price was cheaper = earlier / better timing\n")

def show_wallet(conn: sqlite3.Connection, wallet: str, days: int):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT market_question, winning_side, trade_timestamp,
               entry_price, usdc_spent, profit_usdc, profit_pct,
               market_url, scan_date
        FROM winners
        WHERE LOWER(proxy_wallet) = LOWER(?) AND scan_date >= ?
        ORDER BY trade_timestamp DESC
    """, (wallet, since)).fetchall()

    if not rows:
        print(f"\nNo wins found for {wallet} in the last {days} days.")
        return

    total_profit = sum(r[5] for r in rows)
    total_spent  = sum(r[4] for r in rows)
    roi          = (total_profit / total_spent * 100) if total_spent > 0 else 0

    print(f"\n{'='*70}")
    print(f"  Trader: {wallet}")
    print(f"  {len(rows)} wins  |  Total profit: ${total_profit:,.2f}  |  ROI: {roi:.1f}%")
    print(f"{'='*70}\n")

    for (question, side, ts, entry, spent, profit, pct, url, scan_date) in rows:
        entry_s = f"{entry*100:.1f}%" if entry else "?"
        ts_s    = ts[:16].replace("T", " ") if ts else "?"
        print(f"  [{scan_date}] [{side} won] +${profit:,.2f} ({pct:+.1f}%)")
        print(f"  Bet ${spent:,.2f} @ {entry_s} on {ts_s} UTC")
        print(f"  {question[:68]}")
        print(f"  {url}\n")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Polymarket weather trader insights")
    parser.add_argument("--days",   type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--top",    type=int, default=20, help="How many traders to show (default: 20)")
    parser.add_argument("--wallet", type=str, default="",  help="Deep dive on a specific wallet address")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    if args.wallet:
        show_wallet(conn, args.wallet, args.days)
        conn.close()
        return

    winners = load_winners(conn, args.days)
    if not winners:
        print(f"No winner data found for the last {args.days} days.")
        print("Run polymarket_scanner.py first to populate the database.")
        conn.close()
        return

    print(f"\nLoaded {len(winners)} winning trades across the last {args.days} days")

    traders = aggregate_traders(winners)
    print(f"Found {len(traders)} unique winning traders\n")

    show_consistency(rank_by_consistency(traders, args.top), args.days)
    show_profit(rank_by_total_profit(traders, args.top), args.days)
    show_timing(rank_by_timing(traders, args.top), args.days)

    conn.close()

if __name__ == "__main__":
    main()
