import csv
import os
from collections import defaultdict
from datetime import date

NAV_DIR = "data/nav_history"
OUT_DIR = "data/nav_wide"

CURRENT_YEAR = str(date.today().year)
OUT_FILE = f"{OUT_DIR}/nav_wide_{CURRENT_YEAR}.csv"

os.makedirs(OUT_DIR, exist_ok=True)


# ---------- FAST LAST DATE ----------
def read_last_date(filepath):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "rb") as f:
            f.seek(-256, os.SEEK_END)
            return f.readlines()[-1].decode().split(",")[0]
    except Exception:
        return None


# ---------- READ EXISTING HEADER ----------
def read_existing_header(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return next(reader)[1:]  # skip Date column


# ---------- LOAD ALL SCHEME FILES ----------
scheme_files = sorted(
    f for f in os.listdir(NAV_DIR)
    if f.endswith(".csv")
)

available_schemes = [os.path.splitext(f)[0] for f in scheme_files]
print(f"Available schemes: {len(available_schemes)}")

existing_schemes = read_existing_header(OUT_FILE)
print(f"Existing schemes in output: {len(existing_schemes)}")

# ---------- MERGE OLD + NEW SCHEMES ----------
all_schemes = sorted(set(existing_schemes + available_schemes))

new_schemes = set(all_schemes) - set(existing_schemes)
if new_schemes:
    print(f"New schemes detected: {len(new_schemes)}")
else:
    print("No new schemes detected")

# ---------- CHECK LAST DATE ----------
last_date = read_last_date(OUT_FILE)
print("Last exported date:", last_date)


# ---------- COLLECT DATA ----------
data = defaultdict(dict)

for file in scheme_files:
    scheme_code = os.path.splitext(file)[0]
    file_path = os.path.join(NAV_DIR, file)

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row["Date"]

            if not d.startswith(CURRENT_YEAR):
                continue

            if last_date and d <= last_date:
                continue

            data[d][scheme_code] = row["NAV"]

if not data and not new_schemes:
    print("No updates required ✅")
    exit()


# ---------- REWRITE FILE IF HEADER EXPANDS ----------
rewrite_required = bool(new_schemes)

if rewrite_required and os.path.exists(OUT_FILE):
    print("Expanding columns → rewriting file ⏳")

    with open(OUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        old_rows = list(reader)

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date"] + all_schemes)

        for r in old_rows:
            row = [r["Date"]]
            for code in all_schemes:
                row.append(r.get(code, ""))
            writer.writerow(row)


# ---------- APPEND NEW DATA ----------
write_header = not os.path.exists(OUT_FILE)

with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    if write_header:
        writer.writerow(["Date"] + all_schemes)

    for d in sorted(data.keys()):
        row = [d]
        daily = data[d]
        for code in all_schemes:
            row.append(daily.get(code, ""))
        writer.writerow(row)

print(f"Update complete → {len(data)} new dates added ✅")
