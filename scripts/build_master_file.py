import csv

codes = {}
categories = {}

with open("data/scheme_codes.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        codes[r["SchemeCode"]] = r

with open("data/scheme_categories.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        categories[r["SchemeCode"]] = r

with open("mf_master.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "SchemeCode",
        "AMC",
        "SchemeName",
        "ISIN",
        "NAV",
        "NAVDate",
        "Category",
        "Type",
        "AMC_FundHouse"
    ])

    for code, s in codes.items():
        c = categories.get(code, {})
        writer.writerow([
            code,
            s["AMC"],
            s["SchemeName"],
            s["ISIN"],
            s["NAV"],
            s["NAVDate"],
            c.get("Category", ""),
            c.get("Type", ""),
            c.get("AMC", "")
        ])
