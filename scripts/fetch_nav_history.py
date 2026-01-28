import csv
import requests
import os
import time
from datetime import datetime

CODES_FILE = "data/scheme_codes.csv"
NAV_DIR = "data/nav_history"

REQUEST_DELAY = 0.12

os.makedirs(NAV_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

def read_last_date(filepath):
    if not os.path.exists(filepath):
        return None

    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        if not rows:
            return None
        return rows[-1]["Date"]

with open(CODES_FILE, newline="", encoding="utf-8") as f:
    schemes = list(csv.DictReader(f))

print(f"Total schemes: {len(schemes)}")

for i, scheme in enumerate(schemes, start=1):
    code = scheme["SchemeCode"]
    filepath = f"{NAV_DIR}/{code}.csv"

    print(f"[{i}/{len(schemes)}] Updating NAV → {code}")

    last_date = read_last_date(filepath)

    try:
        r = session.get(
            f"https://api.mfapi.in/mf/{code}",
            timeout=(5, 15)
        )

        if r.status_code != 200:
            continue

        data = r.json().get("data", [])
        if not data:
            continue

        new_rows = []

        for row in reversed(data):  # oldest → newest
            nav_date = datetime.strptime(row["date"], "%d-%m-%Y").date()
            nav_date_str = nav_date.isoformat()

            if last_date and nav_date_str <= last_date:
                continue

            new_rows.append({
                "Date": nav_date_str,
                "NAV": row["nav"]
            })

        if not new_rows:
            print("  ↳ No new NAV")
            continue

        file_exists = os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Date", "NAV"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)

        print(f"  ↳ Added {len(new_rows)} rows")

        time.sleep(REQUEST_DELAY)

    except Exception as e:
        print("  ERROR:", e)

print("NAV update completed ✅")
