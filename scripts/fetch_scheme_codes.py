import requests
import csv

url = "https://www.amfiindia.com/spages/NAVAll.txt"
text = requests.get(url, timeout=20).text

schemes = {}

for line in text.splitlines():
    parts = line.split(";")
    if parts[0].isdigit():
        schemes[parts[0]] = parts[3]

with open("data/scheme_codes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["SchemeCode", "SchemeName"])
    for code, name in sorted(schemes.items()):
        writer.writerow([code, name])

print(f"Saved {len(schemes)} scheme codes")
