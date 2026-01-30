import csv
import requests
import os
import time
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
CODES_FILE = "data/scheme_codes.csv"
NAV_DIR = "data/nav_history"

MAX_WORKERS = 8
REQUEST_DELAY = 0.12
CONNECT_TIMEOUT = 2
READ_TIMEOUT = 5

TODAY = date.today().isoformat()
# ==========================================

print("📁 Ensuring NAV history directory exists...")
os.makedirs(NAV_DIR, exist_ok=True)


# ---------- ULTRA FAST LAST DATE ----------
def read_last_date(filepath):
    if not os.path.exists(filepath):
        print("   ↳ No existing NAV file found")
        return None
    try:
        with open(filepath, "rb") as f:
            f.seek(-256, os.SEEK_END)
            last_line = f.readlines()[-1].decode().strip()
            if last_line and not last_line.startswith("Date"):
                last_date = last_line.split(",")[0]
                print(f"   ↳ Last stored NAV date: {last_date}")
                return last_date
    except Exception:
        print("   ↳ Could not read last date safely")
    return None


# ---------- WORKER FUNCTION ----------
def process_scheme(args):
    i, total, scheme = args
    code = scheme["SchemeCode"]
    filepath = os.path.join(NAV_DIR, f"{code}.csv")

    print(f"\n🔄 [{i}/{total}] Processing scheme {code}")

    last_date = read_last_date(filepath)

    # ✅ Skip API call entirely if already up to date
    if last_date == TODAY:
        print("   ⏭ Already up to date — skipping API call")
        return f"[{i}/{total}] {code} → up to date"

    print("   🌐 Fetching NAV data from API...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (NAV-Updater)"
    })

    try:
        r = session.get(
            f"https://api.mfapi.in/mf/{code}",
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )

        if r.status_code != 200:
            print("   ❌ API returned non-200 status")
            return f"[{i}/{total}] {code} → API error"

        data = r.json().get("data")
        if not data:
            print("   ⚠ API returned empty data")
            return f"[{i}/{total}] {code} → no data"

        print(f"   📦 Total NAV records received: {len(data)}")

        last_date_obj = (
            datetime.fromisoformat(last_date).date()
            if last_date else None
        )

        new_rows = []
        for row in reversed(data):
            nav_date = datetime.strptime(row["date"], "%d-%m-%Y").date()
            if last_date_obj and nav_date <= last_date_obj:
                continue

            new_rows.append({
                "Date": nav_date.isoformat(),
                "NAV": row["nav"]
            })

        print(f"   ➕ New NAV rows after date filter: {len(new_rows)}")

        if not new_rows:
            return f"[{i}/{total}] {code} → no new NAV"

        # ---------- DUPLICATE PREVENTION ----------
        existing_dates = set()
        if os.path.exists(filepath):
            print("   🔍 Checking for duplicate dates...")
            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_dates.add(row["Date"])

        new_rows_filtered = [
            row for row in new_rows
            if row["Date"] not in existing_dates
        ]

        print(f"   🧹 Rows after duplicate removal: {len(new_rows_filtered)}")

        if new_rows_filtered:
            write_header = not os.path.exists(filepath)
            print("   💾 Writing NAV rows to CSV...")
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Date", "NAV"])
                if write_header:
                    writer.writeheader()
                writer.writerows(new_rows_filtered)

        print("   ⏱ Sleeping briefly to respect API limits...")
        time.sleep(REQUEST_DELAY)

        return f"[{i}/{total}] {code} → +{len(new_rows_filtered)} rows"

    except requests.exceptions.RequestException:
        print("   🚫 Network error occurred")
        return f"[{i}/{total}] {code} → network error"
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return f"[{i}/{total}] {code} → error: {e}"


# ---------- LOAD SCHEME CODES ----------
print("\n📄 Loading scheme codes...")
with open(CODES_FILE, newline="", encoding="utf-8") as f:
    schemes = list(csv.DictReader(f))

total = len(schemes)
print(f"📊 Total schemes loaded: {total}")
print(f"⚙️ Parallel workers: {MAX_WORKERS}\n")

tasks = [(i, total, scheme) for i, scheme in enumerate(schemes, start=1)]

# ---------- PARALLEL EXECUTION ----------
print("🚀 Starting NAV update process...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_scheme, t) for t in tasks]
    for future in as_completed(futures):
        print("✅", future.result())

print("\n🎉 NAV history update completed successfully")
