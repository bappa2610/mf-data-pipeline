import csv
import os

DATA_DIR = "data"
MASTER_FILE = os.path.join(DATA_DIR, "mf_master.csv")

os.makedirs(DATA_DIR, exist_ok=True)

# ---------------- LOAD scheme_codes.csv (ORDER PRESERVED) ----------------
codes = []
with open(os.path.join(DATA_DIR, "scheme_codes.csv"), newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        codes.append(r)

# ---------------- LOAD scheme_categories.csv (LOOKUP MAP) ----------------
categories = {}
with open(os.path.join(DATA_DIR, "scheme_categories.csv"), newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        categories[r["SchemeCode"]] = r

# ---------------- WRITE MASTER FILE ----------------
with open(MASTER_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    writer.writerow([
        "SchemeCode",
        "AMC",
        "SchemeName",
        "ISIN",
        "NAV",
        "Date",
        "SchemeType",
        "CategoryRaw",
        "Category",
        "SubCategory"
    ])

    for s in codes:
        code = s["SchemeCode"]
        c = categories.get(code, {})

        writer.writerow([
            code,
            s.get("AMC", ""),
            s.get("SchemeName", ""),
            s.get("ISIN", ""),
            s.get("NAV", ""),
            s.get("Date", ""),
            c.get("SchemeType", ""),
            c.get("CategoryRaw", ""),
            c.get("Category", ""),
            c.get("SubCategory", "")
        ])

print(f"MF master file created at {MASTER_FILE} with {len(codes)} records ✅")
