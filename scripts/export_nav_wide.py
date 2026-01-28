import csv
import os
from collections import defaultdict

NAV_DIR = "data/nav_history"
OUT_DIR = "data/nav_wide"

os.makedirs(OUT_DIR, exist_ok=True)

# ---------------- LOAD SCHEME FILES ---------------- #
scheme_files = sorted(
    f for f in os.listdir(NAV_DIR)
    if f.endswith(".csv")
)

schemes = [os.path.splitext(f)[0] for f in scheme_files]
print(f"Total schemes: {len(schemes)}")

# ---------------- LOAD ALL NAV DATA ---------------- #
# data[year][date][schemecode] = nav
data = defaultdict(lambda: defaultdict(dict))

for file in scheme_files:
    scheme_code = os.path.splitext(file)[0]
    file_path = os.path.join(NAV_DIR, file)

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = row["Date"]
            nav = row["NAV"]
            year = date[:4]

            data[year][date][scheme_code] = nav

# ---------------- WRITE YEARLY WIDE FILES ---------------- #
for year in sorted(data.keys()):
    out_file = f"{OUT_DIR}/nav_wide_{year}.csv"
    print(f"\nProcessing {year}")

    last_date = None

    # -------- CHECK EXISTING FILE -------- #
    if os.path.exists(out_file):
        with open(out_file, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
            if len(rows) > 1:
                last_date = rows[-1][0]

        print("Last exported date:", last_date)

    write_header = not os.path.exists(out_file)

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow(["Date"] + schemes)

        for date in sorted(data[year].keys()):
            if last_date and date <= last_date:
                continue

            row = [date]
            daily = data[year][date]
            for code in schemes:
                row.append(daily.get(code, ""))

            writer.writerow(row)

    print(f"Updated {len(data[year])} total dates")

print("\nNAV wide CSV update completed ✅")
