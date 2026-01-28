import sqlite3
import csv
import os

DB_FILE = "data/mf_nav.db"
OUT_FILE = "data/nav_history_all_schemes.csv"

os.makedirs("data", exist_ok=True)

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# ---------------- GET ALL SCHEME CODES ----------------
cur.execute("SELECT DISTINCT SchemeCode FROM nav_history ORDER BY SchemeCode")
schemes = [row[0] for row in cur.fetchall()]
print(f"Total schemes: {len(schemes)}")

# ---------------- GET ALL DATES ----------------
cur.execute("SELECT DISTINCT Date FROM nav_history ORDER BY Date")
dates = [row[0] for row in cur.fetchall()]
print(f"Total dates: {len(dates)}")

# ---------------- BUILD NAV MAP ----------------
cur.execute("SELECT Date, SchemeCode, NAV FROM nav_history")
nav_map = {}
for date, code, nav in cur.fetchall():
    nav_map.setdefault(date, {})[code] = nav

conn.close()

# ---------------- WRITE WIDE CSV ----------------
with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    # Header: Date + SchemeCodes
    writer.writerow(["Date"] + schemes)

    # Rows: one per date
    for date in dates:
        row = [date]
        daily = nav_map.get(date, {})
        for code in schemes:
            row.append(daily.get(code, ""))  # empty if NAV not available
        writer.writerow(row)

print(f"Master NAV history CSV created: {OUT_FILE} ✅")
