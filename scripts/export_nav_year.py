import csv
import os
from datetime import datetime
from collections import defaultdict

NAV_DIR = "data/nav_history"
OUT_DIR = "data/nav_year"

os.makedirs(OUT_DIR, exist_ok=True)

# ---------------- LOAD SCHEME FILES ----------------
scheme_files = sorted(
    f for f in os.listdir(NAV_DIR)
    if f.endswith(".csv")
)

print(f"Total schemes: {len(scheme_files)}")

# ---------------- LOAD EXISTING DATA (ONCE) ----------------
# existing[year] = set((SchemeCode, Date))
existing = defaultdict(set)

for fname in os.listdir(OUT_DIR):
    if fname.startswith("nav_year_") and fname.endswith(".csv"):
        year = fname.replace("nav_year_", "").replace(".csv", "")

        # 🚫 skip invalid year files
        if not year.isdigit() or len(year) != 4:
            continue

        path = os.path.join(OUT_DIR, fname)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("SchemeCode") and r.get("Date"):
                    existing[year].add((r["SchemeCode"], r["Date"]))

print("Existing year files cached ✅")

# ---------------- COLLECT NEW DATA ----------------
# to_write[year] = list of (SchemeCode, Date, NAV)
to_write = defaultdict(list)

for file in scheme_files:
    scheme_code = os.path.splitext(file)[0]
    file_path = os.path.join(NAV_DIR, file)

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
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

# ---------------- WRITE SORTED OUTPUT ----------------
for year, rows in to_write.items():
    out_file = os.path.join(OUT_DIR, f"nav_year_{year}.csv")
    write_header = not os.path.exists(out_file)

    # ✅ sort by SchemeCode → Date
    rows.sort(key=lambda x: (x[0], x[1]))

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["SchemeCode", "Date", "NAV"])
        writer.writerows(rows)

    print(f"{year}: +{len(rows)} rows")

print("\nNAV year files cleaned & updated correctly ✅")
