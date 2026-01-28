import requests
import csv

url = "https://www.amfiindia.com/spages/NAVAll.txt"
text = requests.get(url, timeout=20).text

rows = {}

current_amc = ""

for line in text.splitlines():
    line = line.strip()
    if not line:
        continue

    # AMC name line
    if ";" not in line:
        current_amc = line
        continue

    parts = line.split(";")

    if not parts[0].isdigit():
        continue

    scheme_code = parts[0]
    isin_growth = parts[2] or parts[1]
    scheme_name = parts[3]
    nav = parts[4]
    nav_date = parts[5]

    rows[scheme_code] = [
        scheme_code,
        current_amc,
        scheme_name,
        isin_growth,
        nav,
        nav_date
    ]

with open("data/scheme_codes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "SchemeCode",
        "AMC",
        "SchemeName",
        "ISIN",
        "NAV",
        "NAVDate"
    ])
    for r in rows.values():
        writer.writerow(r)

print(f"Saved {len(rows)} schemes with NAV data")
