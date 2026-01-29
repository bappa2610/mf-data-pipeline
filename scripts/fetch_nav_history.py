import csv
import requests
import os
import time
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
CODES_FILE = "data/scheme_codes.csv"
NAV_DIR = "data/nav_history"

MAX_WORKERS = 8          # 🔧 change workers here
REQUEST_DELAY = 0.12     # 🔧 API-friendly delay
CONNECT_TIMEOUT = 2
READ_TIMEOUT = 5

TODAY = date.today().isoformat()
# ==========================================

os.makedirs(NAV_DIR, exist_ok=True)


# ---------- ULTRA FAST LAST DATE ----------
def read_last_date(filepath):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "rb") as f:
            f.seek(-256, os.SEEK_END)
            last_line = f.readlines()[-1].decode().strip()
            if last_line and not last_line.startswith("Date"):
                return last_line.split(",")[0]
    except Exception:
        pass
    return None


# ---------- WORKER FUNCTION ----------
def process_scheme(args):
    i, total, scheme = args
    code = scheme["SchemeCode"]
    filepath = os.path.join(NAV_DIR, f"{code}.csv")

    last_date = read_last_date(filepath)

    # ✅ Skip API call entirely
    if last_date == TODAY:
        return f"[{i}/{total}] {code} → up to date"

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
            return f"[{i}/{total}] {code} → API error"

        data = r.json().get("data")
        if not data:
            return f"[{i}/{total}] {code} → no data"

        last_date_obj = (
            datetime.fromisoformat(last_date).date()
            if last_date else None
        )

        new_rows = []
        for row in reversed(data):  # oldest → newest
            nav_date = datetime.strptime(row["date"], "%d-%m-%Y").date()
            if last_date_obj and nav_date <= last_date_obj:
                continue

            new_rows.append({
                "Date": nav_date.isoformat(),
                "NAV": row["nav"]
            })

        if not new_rows:
            return f"[{i}/{total}] {code} → no new NAV"

        write_header = not os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Date", "NAV"])
            if write_header:
                writer.writeheader()
            writer.writerows(new_rows)

        time.sleep(REQUEST_DELAY)  # ✅ sleep only after write

        return f"[{i}/{total}] {code} → +{len(new_rows)} rows"

    except requests.exceptions.RequestException:
        return f"[{i}/{total}] {code} → network error"
    except Exception as e:
        return f"[{i}/{total}] {code} → error: {e}"


# ---------- LOAD SCHEME CODES (ORDER PRESERVED) ----------
with open(CODES_FILE, newline="", encoding="utf-8") as f:
    schemes = list(csv.DictReader(f))

total = len(schemes)
print(f"Total schemes: {total}")
print(f"Workers: {MAX_WORKERS}\n")

tasks = [(i, total, scheme) for i, scheme in enumerate(schemes, start=1)]

# ---------- PARALLEL EXECUTION ----------
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_scheme, t) for t in tasks]
    for future in as_completed(futures):
        print(future.result())

print("\nNAV history update completed ✅")
