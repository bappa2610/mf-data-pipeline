import csv
import requests
import os
import time
from datetime import datetime, date

CODES_FILE = "data/scheme_codes.csv"
NAV_DIR = "data/nav_history"

REQUEST_DELAY = 0.12   # sleep only when data is written
TODAY = date.today().isoformat()

os.makedirs(NAV_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (NAV-Updater)"
})

# ---------- FAST LAST DATE READER ----------
def read_last_date(filepath):
    if not os.path.exists(filepath):
        return None

    try:
        with open(filepath, "rb") as f:
            f.seek(-1024, os.SEEK_END)
            lines = f.read().decode().splitlines()
            if len(lines) > 1:
                return lines[-1].split(",")[0]
    except Exception:
        pass

    return None


# ---------- LOAD SCHEME CODES ----------
with open(CODES_FILE, newline="", encoding="utf-8") as f:
    schemes = list(csv.DictReader(f))

print(f"Total schemes: {len(schemes)}\n")

# ---------- PROCESS EACH SCHEME ----------
for i, scheme in enumerate(schemes, start=1):
    code = scheme["SchemeCode"]
    filepath = f"{NAV_DIR}/{code}.csv"

    print(f"[{i}/{len(schemes)}] Scheme {code}")

    last_date = read_last_date(filepath)

    # ✅ Skip if already updated today
    if last_date == TODAY:
        print("  ↳ Already up to date")
        continue

    try:
        r = session.get(
            f"https://api.mfapi.in/mf/{code}",
            timeout=(3, 8)
        )

        if r.status_code != 200:
            print("  ↳ API error")
            continue

        data = r.json().get("data", [])
        if not data:
            print("  ↳ No NAV data")
            continue

        new_rows = []

        # API returns latest → oldest, reverse it
        for row in reversed(data):
            nav_date = datetime.strptime(row["date"], "%d-%m-%Y").date().isoformat()

            if last_date and nav_date <= last_date:
                continue

            new_rows.append({
                "Date": nav_date,
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

        # ✅ Sleep ONLY when data is written
        time.sleep(REQUEST_DELAY)

    except requests.exceptions.RequestException as e:
        print("  ↳ Network error:", e)
    except Exception as e:
        print("  ↳ Error:", e)

print("\nNAV history update completed ✅")
