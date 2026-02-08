import csv
import os
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

NAV_DIR = "data/NAV/nav_history"
OUT_DIR = "data/NAV/nav_year"

print("📁 Preparing yearly NAV output directory...")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------- LOAD SCHEME FILES ----------------
scheme_files = sorted(
    f for f in os.listdir(NAV_DIR)
    if f.endswith(".csv")
)

print(f"📊 Schemes detected: {len(scheme_files)}")

# ---------------- LOAD EXISTING DATA ----------------
print("🗂 Loading existing yearly NAV indexes...")
existing = defaultdict(set)

year_files = sorted(
    f for f in os.listdir(OUT_DIR)
    if f.startswith("nav_year_") and f.endswith(".csv")
)

if not year_files:
    print("ℹ️ No existing yearly NAV files found")

for fname in tqdm(year_files, desc="Loading existing yearly NAV indexes"):
    year = fname.replace("nav_year_", "").replace(".csv", "")
    if not year.isdigit() or len(year) != 4:
        continue

    path = os.path.join(OUT_DIR, fname)
    count = 0

    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("SchemeCode") and r.get("Date"):
                existing[year].add((r["SchemeCode"], r["Date"]))
                count += 1

print("✅ Existing yearly NAV index ready\n")

# ---------------- COLLECT NEW DATA ----------------
to_write = defaultdict(list)

print("\n🔍 Processing schemes...")

for file in tqdm(scheme_files, desc="Processing schemes"):
    scheme_code = os.path.splitext(file)[0]
    file_path = os.path.join(NAV_DIR, file)

    scanned = added = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            scanned += 1
            raw_date = row.get("Date")
            nav = row.get("NAV")

            if not raw_date or not nav:
                continue

            try:
                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError:
                continue

            year = str(d.year)
            date_str = d.isoformat()
            key = (scheme_code, date_str)

            if key in existing[year]:
                continue

            existing[year].add(key)
            to_write[year].append((scheme_code, date_str, nav))
            added += 1

# ---------------- WRITE OUTPUT ----------------
print("\n💾 Writing yearly NAV files...")

for year, rows in to_write.items():
    out_file = os.path.join(OUT_DIR, f"nav_year_{year}.csv")
    write_header = not os.path.exists(out_file)

    rows.sort(key=lambda x: (x[0], x[1]))

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["SchemeCode", "Date", "NAV"])
        writer.writerows(rows)

    print(f"📅 {year} → ✍️ {len(rows)} rows")

print("\n🎉 Year-wise NAV files updated successfully ✅")
