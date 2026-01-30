import csv
import os
from datetime import datetime
from collections import defaultdict

NAV_DIR = "data/nav_history"
OUT_DIR = "data/nav_year"

print("📁 Ensuring yearly NAV output directory exists...")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------- LOAD SCHEME FILES ----------------
print("\n📄 Scanning NAV history directory...")
scheme_files = sorted(
    f for f in os.listdir(NAV_DIR)
    if f.endswith(".csv")
)

print(f"📊 Total scheme NAV files found: {len(scheme_files)}")

# ---------------- LOAD EXISTING DATA (ONCE) ----------------
print("\n🗂 Caching existing yearly NAV data...")
# existing[year] = set((SchemeCode, Date))
existing = defaultdict(set)

for fname in os.listdir(OUT_DIR):
    if fname.startswith("nav_year_") and fname.endswith(".csv"):
        year = fname.replace("nav_year_", "").replace(".csv", "")

        # 🚫 skip invalid year files
        if not year.isdigit() or len(year) != 4:
            print(f"   ⏭ Skipping invalid file: {fname}")
            continue

        path = os.path.join(OUT_DIR, fname)
        print(f"   📂 Loading existing data for year {year}")

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for r in reader:
                if r.get("SchemeCode") and r.get("Date"):
                    existing[year].add((r["SchemeCode"], r["Date"]))
                    count += 1

        print(f"      ↳ Cached {count} rows")

print("✅ Existing year files cached\n")

# ---------------- COLLECT NEW DATA ----------------
print("🔍 Collecting new NAV rows from scheme files...")
# to_write[year] = list of (SchemeCode, Date, NAV)
to_write = defaultdict(list)

for idx, file in enumerate(scheme_files, start=1):
    scheme_code = os.path.splitext(file)[0]
    file_path = os.path.join(NAV_DIR, file)

    print(f"\n🔄 [{idx}/{len(scheme_files)}] Processing scheme {scheme_code}")

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        scanned = 0
        added = 0

        for row in reader:
            scanned += 1
            raw_date = row.get("Date")
            nav = row.get("NAV")

            if not raw_date or not nav:
                continue

            # ✅ Parse ISO date ONLY (YYYY-MM-DD)
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

        print(f"   📖 Rows scanned: {scanned}")
        print(f"   ➕ New rows queued: {added}")

# ---------------- WRITE SORTED OUTPUT ----------------
print("\n💾 Writing yearly NAV files...")

for year, rows in to_write.items():
    out_file = os.path.join(OUT_DIR, f"nav_year_{year}.csv")
    write_header = not os.path.exists(out_file)

    # ✅ sort by SchemeCode → Date
    rows.sort(key=lambda x: (x[0], x[1]))

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            print(f"   🆕 Creating nav_year_{year}.csv with header")
            writer.writerow(["SchemeCode", "Date", "NAV"])
        writer.writerows(rows)

    print(f"   📅 {year}: +{len(rows)} rows written")

print("\n🎉 NAV year files cleaned & updated correctly")
