import csv
import os

NAV_DIR = "data/nav_history"
OUTPUT_FILE = "data/nav_history_all.csv"

rows = []

for filename in sorted(os.listdir(NAV_DIR)):
    if not filename.endswith(".csv"):
        continue

    scheme_code = filename.replace(".csv", "")
    filepath = os.path.join(NAV_DIR, filename)

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "SchemeCode": scheme_code,
                "Date": row["Date"],
                "NAV": row["NAV"]
            })

# Write combined file
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["SchemeCode", "Date", "NAV"]
    )
    writer.writeheader()
    writer.writerows(rows)

print("Combined NAV history created ✅")
