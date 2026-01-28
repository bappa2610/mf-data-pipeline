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

def get_last_saved_date(filepath):
    """Return last saved NAV date (YYYY-MM-DD) or None"""
    if not os.path.exists(filepath):
        return None

    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        if not rows:
            return None
        return rows[-1]["Date"]

with open(CODES_FILE, newline="", encoding="utf-8") as f:
    schemes = list(csv.DictReader(f))

total = len(schemes)
print(f"Total schemes in master: {total}")

for idx, scheme in enumerate(schemes, start=1):
    scheme_code = scheme["SchemeCode"]
    filepath = f"{NAV_DIR}/{scheme_code}.csv"

    last_date = get_last_saved_date(filepath)
    is_new_scheme = last_date is None

    print(
        f"[{idx}/{total}] "
        f"{scheme_code} | "
        f"{'NEW' if is_new_scheme else 'UPDATE'}"
    )

    try:
        r = session.get(
            f"https://api.mfapi.in/mf/{scheme_code}",
            timeout=(5, 15)
        )

        if r.status_code != 200:
            continue

        api_data = r.json().get("data", [])
        if not api_data:
            continue

        new_rows = []

        # API gives latest first → reverse for chronological append
        for row in reversed(api_data):
            nav_date = datetime.strptime(
                row["date"], "%d-%m-%Y"
            ).date().isoformat()

            if last_date and nav_date <= last_date:
                continue

            new_rows.append({
                "Date": nav_date,
                "NAV": row["nav"]
            })

        if not new_rows:
            print("   ↳ No new NAV")
            continue

        file_exists = os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Date", "NAV"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)

        print(f"   ↳ Added {len(new_rows)} rows")

        time.sleep(REQUEST_DELAY)

    except Exception as e:
        print("   ERROR:", e)

print("\nNAV history update completed ✅")
