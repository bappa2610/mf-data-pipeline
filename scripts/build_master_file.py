import csv
import os

codes = {}
categories = {}

DATA_DIR = "data"
MASTER_FILE = os.path.join(DATA_DIR, "mf_master.csv")

# ---------------- ENSURE DATA FOLDER EXISTS ----------------
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------- LOAD scheme_codes.csv ----------------
with open(os.path.join(DATA_DIR, "scheme_codes.csv"), newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        codes[r["SchemeCode"]] = r

# ---------------- LOAD scheme_categories.csv ----------------
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
        "NAVDate",
        "SchemeType",
        "CategoryRaw",
        "Category",
        "SubCategory"
    ])

    for code, s in codes.items():
        c = categories.get(code, {})

        writer.writerow([
            code,
            s.get("AMC", ""),
            s.get("SchemeName", ""),
            s.get("ISIN", ""),
            s.get("NAV", ""),
            s.get("NAVDate", ""),
            c.get("SchemeType", ""),
            c.get("CategoryRaw", ""),
            c.get("Category", ""),
            c.get("SubCategory", "")
        ])

print(f"MF master file created at {MASTER_FILE} with {len(codes)} records ✅")
