import urllib.request, json

url = "https://gamma-api.polymarket.com/search?query=coldmath&type=profiles"
req = urllib.request.Request(url, headers={"User-Agent": "test"})
with urllib.request.urlopen(req) as r:
    data = json.loads(r.read())
    print(json.dumps(data, indent=2))