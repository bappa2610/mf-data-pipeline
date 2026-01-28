import sqlite3
import csv
import os

DB_FILE = "data/mf_nav.db"
OUT_DIR = "data/nav_wide"

os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Get all scheme codes (fixed column order)
cur.execute("""
    SELECT DISTINCT SchemeCode
    FROM nav_history
    ORDER BY SchemeCode
""")
schemes = [r[0] for r in cur.fetchall()]

# Get available years from DB
cur.execute("""
    SELECT DISTINCT substr(Date,1,4)
    FROM nav_history
    ORDER BY 1
""")
years = [r[0] for r in cur.fetchall()]

for year in years:
    out_file = f"{OUT_DIR}/nav_wide_{year}.csv"
    print(f"\nProcessing {year}")

    last_date = None

    # -------- CHECK EXISTING FILE -------- #
    if os.path.exists(out_file):
        with open(out_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if len(rows) > 1:
                last_date = rows[-1][0]   # last date already exported

        print("Last exported date:", last_date)

    # -------- FETCH ONLY NEW DATA -------- #
    if last_date:
        cur.execute("""
            SELECT Date, SchemeCode, NAV
            FROM nav_history
            WHERE Date > ?
              AND Date LIKE ?
            ORDER BY Date
        """, (last_date, f"{year}%"))
    else:
        cur.execute("""
            SELECT Date, SchemeCode, NAV
            FROM nav_history
            WHERE Date LIKE ?
            ORDER BY Date
        """, (f"{year}%",))

    rows = cur.fetchall()

    if not rows:
        print("No new NAVs → skipping")
        continue

    # -------- BUILD DATE MAP -------- #
    data = {}
    for date, code, nav in rows:
        data.setdefault(date, {})[code] = nav

    write_header = not os.path.exists(out_file)

    # -------- WRITE / APPEND -------- #
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow(["Date"] + schemes)

        for date in sorted(data.keys()):
            row = [date]
            daily = data[date]
            for code in schemes:
                row.append(daily.get(code, ""))
            writer.writerow(row)

    print(f"Updated {len(data)} new dates")

conn.close()
print("\nNAV wide update completed ✅")
