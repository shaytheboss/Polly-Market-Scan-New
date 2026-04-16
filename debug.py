import urllib.request, json

cid = "0xbe47c4384e8086b14a14bf17ca1345f47a6578ae064ccc4dd5330794b8f355fa"

# outcomeIndex=1 = NO, שהוא הצד המנצח בשוק הזה
url = f"https://data-api.polymarket.com/trades?market={cid}&side=BUY&outcomeIndex=1&limit=10"
req = urllib.request.Request(url, headers={"User-Agent": "test"})
with urllib.request.urlopen(req) as r:
    trades = json.loads(r.read())
    print(f"NO buyers: {len(trades)}")
    for t in trades[:3]:
        print(f"  usdcSize={t.get('usdcSize')}  size={t.get('size')}  price={t.get('price')}  pseudonym={t.get('pseudonym')}")