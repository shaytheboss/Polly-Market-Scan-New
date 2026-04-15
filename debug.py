import urllib.request, json

slug = "will-the-fed-decrease-interest-rates-by-50-bps-after-the-april-2"  # קח slug אמיתי מהתוצאות
url = f"https://gamma-api.polymarket.com/markets/{slug}/history?interval=1d&fidelity=60"
req = urllib.request.Request(url, headers={"User-Agent": "test"})
with urllib.request.urlopen(req) as r:
    print(r.status)
    print(json.loads(r.read())[:3])
