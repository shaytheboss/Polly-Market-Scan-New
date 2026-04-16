import urllib.request, json

# קודם נביא את השוק ונראה את ה-conditionId שלו
url = "https://gamma-api.polymarket.com/markets?slug=highest-temperature-in-los-angeles-on-april-15-2026-68-69f"
req = urllib.request.Request(url, headers={"User-Agent": "test"})
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read())
    m = data[0]
    cid = m.get("conditionId")
    print(f"conditionId: {cid}")
    print(f"clobTokenIds: {m.get('clobTokenIds')}")

# ננסה לשלוף טריידים עם conditionId
url2 = f"https://data-api.polymarket.com/trades?conditionId={cid}&limit=5"
req2 = urllib.request.Request(url2, headers={"User-Agent": "test"})
with urllib.request.urlopen(req2) as r:
    trades = json.loads(r.read())
    print(f"\nTrades with conditionId: {len(trades)}")
    if trades:
        print(json.dumps(trades[0], indent=2))

# ננסה גם עם market במקום conditionId
url3 = f"https://data-api.polymarket.com/trades?market={cid}&limit=5"
req3 = urllib.request.Request(url3, headers={"User-Agent": "test"})
with urllib.request.urlopen(req3) as r:
    trades2 = json.loads(r.read())
    print(f"\nTrades with market=: {len(trades2)}")
    if trades2:
        print(json.dumps(trades2[0], indent=2))