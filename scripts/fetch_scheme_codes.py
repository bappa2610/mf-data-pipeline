import requests
import csv
import os

URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUT_FILE = "data/scheme_codes.csv"

print("📁 Preparing data directory...")
os.makedirs("data", exist_ok=True)
print("✅ Data directory ready\n")

print("🌐 Fetching NAVAll.txt from AMFI...")
response = requests.get(URL, timeout=20)
response.raise_for_status()
print("✅ Download completed\n")

text = response.text

current_amc = ""
rows = {}

print("📖 Parsing NAV data...\n")

for line_no, line in enumerate(text.splitlines(), start=1):
    line = line.strip()

    if not line:
        continue

    parts = line.split(";")

    # ---------- AMC NAME ----------
    if len(parts) == 1 and not parts[0].isdigit():
        current_amc = parts[0].strip()
        print(f"🏢 AMC Detected: {current_amc}")
        continue

    # ---------- SCHEME DATA ----------
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

        # ✅ Clean one-line output
        print(f"📄 {scheme_name}")

print(f"\n🧮 Total schemes parsed: {len(rows)}")

# ---------- WRITE CSV ----------
print(f"\n💾 Saving scheme master file → {OUT_FILE}\n")

with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["SchemeCode", "AMC", "SchemeName", "ISIN", "NAV", "Date"]
    )
    writer.writeheader()

    for code in sorted(rows.keys(), key=int):
        writer.writerow(rows[code])

print(f"🎉 Successfully saved {len(rows)} schemes")
print("📦 scheme_codes.csv is ready for use ✅")
