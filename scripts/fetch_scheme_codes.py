import requests
import csv
import os

URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUT_FILE = "data/scheme_codes.csv"

os.makedirs("data", exist_ok=True)

response = requests.get(URL, timeout=20)
response.raise_for_status()
text = response.text

current_amc = ""
rows = {}

for line in text.splitlines():
    line = line.strip()

    # Skip empty lines
    if not line:
        continue

    parts = line.split(";")

    # ---------- AMC NAME LINE ----------
    if len(parts) == 1 and not parts[0].isdigit():
        current_amc = parts[0].strip()
        continue

    # ---------- SCHEME DATA LINE ----------
    if len(parts) >= 6 and parts[0].isdigit():
        scheme_code = parts[0].strip()
        isin = parts[1].strip() or parts[2].strip()
        scheme_name = parts[3].strip()
        nav = parts[4].strip()
        nav_date = parts[5].strip()

        rows[scheme_code] = {
            "SchemeCode": scheme_code,
            "AMC": current_amc,
            "SchemeName": scheme_name,
            "ISIN": isin,
            "NAV": nav,
            "Date": nav_date
        }

# ---------- WRITE CSV ----------
with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["SchemeCode", "AMC", "SchemeName", "ISIN", "NAV", "Date"]
    )
    writer.writeheader()

    for code in sorted(rows.keys(), key=int):
        writer.writerow(rows[code])

print(f"✅ Saved {len(rows)} scheme codes to {OUT_FILE}")
