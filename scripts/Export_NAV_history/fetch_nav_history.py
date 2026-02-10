import csv
import json
import os
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ================= CONFIG =================
CODES_FILE = "data/scheme_data/MF_data/amfi_mf_analyzed_schemes.csv"
NAV_DIR = "data/NAV/nav_history"
LOCAL_DATA_DIR = "data/scheme_data/RAW_data/all_funds"

MAX_WORKERS = 10

TODAY = date.today().isoformat()
# ==========================================

print("📁 Checking NAV history directory...")
os.makedirs(NAV_DIR, exist_ok=True)
print("✅ NAV history directory ready\n")


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
    json_filepath = os.path.join(LOCAL_DATA_DIR, f"{code}.json")

    status_line = f"[{i}/{total}] 📌 Scheme {code}"
    result_line = ""

    last_date = read_last_date(filepath)

    if last_date == TODAY:
        result_line = "🟢 Up to date (local data skipped)"
        return status_line, result_line

    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f).get("data")
        if not data:
            return status_line, "⚠️ No NAV data"

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

        if not new_rows:
            return status_line, "🟡 No new NAVs"

        existing_dates = set()
        if os.path.exists(filepath):
            with open(filepath, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    existing_dates.add(r["Date"])

        new_rows_filtered = [
            r for r in new_rows if r["Date"] not in existing_dates
        ]

        if new_rows_filtered:
            write_header = not os.path.exists(filepath)
            with open(filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["Date", "NAV"])
                if write_header:
                    writer.writeheader()
                writer.writerows(new_rows_filtered)

        result_line = f"✅ Updated | +{len(new_rows_filtered)} NAV rows"
        return status_line, result_line

    except Exception as e:
        return status_line, f"❌ Error ({e})"


# ---------- LOAD SCHEME CODES ----------
print("📄 Loading scheme codes from local data...")
scheme_codes = []
for filename in os.listdir(LOCAL_DATA_DIR):
    if filename.endswith('.json'):
        scheme_code = filename[:-5]  # Remove .json
        scheme_codes.append(scheme_code)

schemes = [{"SchemeCode": code} for code in scheme_codes]

total = len(schemes)
print(f"📊 Total schemes found: {total}")
print(f"⚙️ Parallel workers: {MAX_WORKERS}\n")

tasks = [(i, total, scheme) for i, scheme in enumerate(schemes, start=1)]


# ---------- PARALLEL EXECUTION ----------
print("🚀 Starting NAV history update...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_scheme, t) for t in tasks]
    with tqdm(total=total, desc="Processing schemes") as pbar:
        for future in as_completed(futures):
            line1, line2 = future.result()
            pbar.update(1)


print("\n🎉 NAV history update completed successfully ✅")
print("📦 All available NAV data is now up to date\n")
