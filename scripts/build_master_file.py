import csv

codes = {}
categories = {}

with open("data/scheme_codes.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        codes[r["SchemeCode"]] = r["SchemeName"]

with open("data/scheme_categories.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        categories[r["SchemeCode"]] = r

with open("mf_master.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["SchemeCode", "SchemeName", "Category", "Type", "AMC"])

    for code, name in codes.items():
        cat = categories.get(code, {})
        writer.writerow([
            code,
            name,
            cat.get("Category", ""),
            cat.get("Type", ""),
            cat.get("AMC", "")
        ])
