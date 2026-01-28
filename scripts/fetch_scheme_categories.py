import csv
import requests
import time
import os

CATEGORY_FILE = "data/scheme_categories.csv"
CODE_FILE = "data/scheme_codes.csv"

existing = {}

# Load existing categories (if file exists)
if os.path.exists(CATEGORY_FILE):
    with open(CATEGORY_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing[row["SchemeCode"]] = row

# Load scheme codes
with open(CODE_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    codes = list(reader)

new_rows = []

for row in codes:
    code = row["SchemeCode"]

    if code in existing:
        continue   # already fetched

    try:
        r = requests.get(f"https://api.mfapi.in/mf/{code}", timeout=10)
        if r.status_code != 200:
            continue

        meta = r.json().get("meta", {})
        if not meta:
            continue

        new_rows.append({
            "SchemeCode": code,
            "Category": meta.get("scheme_category", ""),
            "Type": meta.get("scheme_type", ""),
            "AMC": meta.get("fund_house", "")
        })

        time.sleep(0.15)

    except:
        pass

# Write updated category file
with open(CATEGORY_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["SchemeCode", "Category", "Type", "AMC"]
    )
    writer.writeheader()
    for v in existing.values():
        writer.writerow(v)
    for v in new_rows:
        writer.writerow(v)

print(f"Added {len(new_rows)} new categories")
