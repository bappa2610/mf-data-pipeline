import requests
import csv
import os

URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUT_FILE = "Test/test.csv"

def parse_scheme_category(category):
    if "(" in category:
        scheme_type, rest = category.split("(", 1)
        rest = rest.rstrip(")")
        if "-" in rest:
            category_part, sub_category = rest.split("-", 1)
            category_part = category_part.strip()
            sub_category = sub_category.strip()
        else:
            category_part = rest.strip()
            sub_category = category_part
    else:
        scheme_type = category
        category_part = ""
        sub_category = ""
    return scheme_type, category_part, sub_category

print("📁 Preparing Test directory...")
os.makedirs("Test", exist_ok=True)
print("✅ Test directory ready\n")

print("🌐 Fetching NAVAll.txt from AMFI...")
try:
    response = requests.get(URL, timeout=20)
    response.raise_for_status()
    print("✅ Download completed\n")
except requests.RequestException as e:
    print(f"❌ Error fetching data: {e}")
    exit(1)

text = response.text

current_amc = ""
current_category = ""
rows = []

print("📖 Parsing NAV data...\n")

for line_no, line in enumerate(text.splitlines(), start=1):
    line = line.strip()

    if not line:
        continue

    parts = line.split(";")

    # ---------- AMC NAME OR CATEGORY ----------
    if len(parts) == 1 and not parts[0].isdigit():
        line_text = parts[0].strip()
        if "Schemes" in line_text:
            current_category = line_text
            print(f"📂 Category Detected: {current_category}")
        else:
            current_amc = line_text
            print(f"🏢 AMC Detected: {current_amc}")
        continue

    # ---------- SCHEME DATA ----------
    if len(parts) >= 6 and parts[0].isdigit():
        scheme_code = parts[0].strip()
        isin = parts[1].strip() or parts[2].strip()
        scheme_name = parts[3].strip()
        nav = parts[4].strip()
        nav_date = parts[5].strip()

        scheme_type, category, sub_category = parse_scheme_category(current_category)

        rows.append({
            "SchemeCode": scheme_code,
            "AMC": current_amc,
            "SchemeName": scheme_name,
            "ISIN": isin,
            "NAV": nav,
            "Date": nav_date,
            "SchemeCategory": current_category,
            "SchemeType": scheme_type,
            "Category": category,
            "SubCategory": sub_category
        })

        # ✅ Clean one-line output
        print(f"📄 {scheme_name}")

print(f"\n🧮 Total schemes parsed: {len(rows)}")

# ---------- WRITE CSV ----------
print(f"\n💾 Saving test data file → {OUT_FILE}\n")

try:
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["SchemeCode", "AMC", "SchemeName", "ISIN", "NAV", "Date", "SchemeCategory", "SchemeType", "Category", "SubCategory"]
        )
        writer.writeheader()

        for row in rows:
            writer.writerow(row)

    print(f"🎉 Successfully saved {len(rows)} schemes")
    print("📦 test.csv is ready for use ✅")
except IOError as e:
    print(f"❌ Error writing CSV: {e}")
    exit(1)
