import csv
import os
import sqlite3

NAV_DIR = "data/nav_history"
DB_FILE = "data/mf_nav.db"

os.makedirs("data", exist_ok=True)

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# ---------- CREATE TABLE ---------- #
cur.execute("""
CREATE TABLE IF NOT EXISTS nav_history (
    SchemeCode TEXT NOT NULL,
    Date TEXT NOT NULL,
    NAV REAL,
    PRIMARY KEY (SchemeCode, Date)
)
""")

# ---------- INSERT DATA ---------- #
inserted = 0

for fname in os.listdir(NAV_DIR):
    if not fname.endswith(".csv"):
        continue

    scheme_code = fname.replace(".csv", "")
    path = os.path.join(NAV_DIR, fname)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO nav_history VALUES (?, ?, ?)",
                    (scheme_code, row["Date"], row["NAV"])
                )
                if cur.rowcount:
                    inserted += 1
            except Exception:
                pass

conn.commit()
conn.close()

print(f"SQLite NAV build complete. Rows inserted: {inserted}")
