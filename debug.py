import urllib.request, json

# קודם נביא שוק אמיתי ונראה מה המבנה שלו
url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1"
req = urllib.request.Request(url, headers={"User-Agent": "test"})
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read())
    market = data[0]
    print("=== כל השדות של שוק ===")
    for key, val in market.items():
        print(f"  {key}: {val}")
