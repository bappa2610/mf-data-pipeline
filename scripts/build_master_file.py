import csv

codes = {}
categories = {}

# Load scheme_codes.csv
with open("data/scheme_codes.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        codes[r["SchemeCode"]] = r

# Load scheme_categories.csv
with open("data/scheme_categories.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        categories[r["SchemeCode"]] = r

# Write final master file
with open("mf_master.csv", "w", newline="", encoding="utf-8") as f:
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

print(f"MF master file created with {len(codes)} records")
