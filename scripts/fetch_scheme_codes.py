import requests
import csv
import os

URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUT_FILE = "data/scheme_codes.csv"

print("📁 Creating data directory if not exists...")
os.makedirs("data", exist_ok=True)

print("🌐 Downloading NAVAll.txt from AMFI...")
response = requests.get(URL, timeout=20)
response.raise_for_status()
print("✅ Download successful")

text = response.text

current_amc = ""
rows = {}

print("📖 Starting to parse NAV data...\n")

for line_no, line in enumerate(text.splitlines(), start=1):
    line = line.strip()

    # Skip empty lines
    if not line:
        continue

    parts = line.split(";")

    # ---------- AMC NAME LINE ----------
    if len(parts) == 1 and not parts[0].isdigit():
        current_amc = parts[0].strip()
        print(f"🏢 Line {line_no}: AMC detected → {current_amc}")
        continue

    # ---------- SCHEME DATA LINE ----------
    if len(parts) >= 6 and parts[0].isdigit():
        scheme_code = parts[0].strip()
        isin = parts[1].strip() or parts[2].strip()
        scheme_name = parts[3].strip()
        nav = parts[4].strip()
        nav_date = parts[5].strip()

        print(f"""
📄 Line {line_no}: Scheme Found
   ├─ Scheme Code : {scheme_code}
   ├─ AMC         : {current_amc}
   ├─ Scheme Name : {scheme_name}
   ├─ ISIN        : {isin}
   ├─ NAV         : {nav}
   └─ Date        : {nav_date}
""")

        rows[scheme_code] = {
            "SchemeCode": scheme_code,
            "AMC": current_amc,
            "SchemeName": scheme_name,
            "ISIN": isin,
            "NAV": nav,
            "Date": nav_date
        }

print(f"\n🧮 Total schemes parsed: {len(rows)}")

# ---------- WRITE CSV ----------
print(f"\n💾 Writing data to CSV → {OUT_FILE}")

with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["SchemeCode", "AMC", "SchemeName", "ISIN", "NAV", "Date"]
    )
    writer.writeheader()

    for code in sorted(rows.keys(), key=int):
        print(f"✍️ Writing SchemeCode {code} to CSV")
        writer.writerow(rows[code])

print(f"\n✅ Saved {len(rows)} scheme codes to {OUT_FILE}")
