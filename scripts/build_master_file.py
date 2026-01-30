import csv
import os

DATA_DIR = "data"
MASTER_FILE = os.path.join(DATA_DIR, "mf_master.csv")

os.makedirs(DATA_DIR, exist_ok=True)
print(f"📁 Ensured data directory exists: {DATA_DIR}\n")

# ---------------- LOAD scheme_codes.csv (ORDER PRESERVED) ----------------
codes = []
scheme_codes_file = os.path.join(DATA_DIR, "scheme_codes.csv")
print(f"📄 Loading scheme codes from {scheme_codes_file} ...")
with open(scheme_codes_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        codes.append(r)

print(f"✅ Loaded {len(codes)} scheme codes\n")

# ---------------- LOAD scheme_categories.csv (LOOKUP MAP) ----------------
categories = {}
categories_file = os.path.join(DATA_DIR, "scheme_categories.csv")
print(f"📄 Loading scheme categories from {categories_file} ...")
with open(categories_file, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        categories[r["SchemeCode"]] = r

print(f"✅ Loaded {len(categories)} scheme categories\n")

# ---------------- WRITE MASTER FILE ----------------
print(f"✍️ Writing master file to {MASTER_FILE} ...\n")
with open(MASTER_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    # Write header
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
    print("📋 Header written")

    # Write each scheme
    for i, s in enumerate(codes, start=1):
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

        if i % 50 == 0 or i == len(codes):
            print(f"   📝 Written {i}/{len(codes)} records...")

print(f"\n🎉 MF master file created at {MASTER_FILE} with {len(codes)} records ✅")
