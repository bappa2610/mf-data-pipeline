import requests
import csv
import re
import os

# Configuration Constants
URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUT_FILE = r"data\scheme_data\v2\amfi_mf_analyzed_schemes.csv"
CSV_FIELDNAMES = ["SchemeCode", "AMC", "SchemeName", "ISIN", "NAV", "Date", "SchemeType", "Category", "SubCategory", "PlanType", "PlanOption", "IDCW_Frequency"]

# Plan Types
PLAN_TYPES = {
    "DIRECT": "Direct",
    "REGULAR": "Regular",
    "RETAIL": "Retail",
    "INSTITUTIONAL": "Institutional",
    "PREMIUM": "Premium",
    "STANDARD": "Standard"
}

# Option Types
OPTION_TYPES = {
    "GROWTH": "Growth",
    "IDCW": "IDCW",
    "BONUS": "Bonus"
}

# IDCW Frequency Keywords
IDCW_FREQUENCIES = {
    "REINVESTMENT": "Reinvestment",
    "MONTHLY": "Monthly",
    "QUARTERLY": "Quarterly",
    "ANNUAL": "Annual",
    "YEARLY": "Annual",
    "WEEKLY": "Weekly",
    "DAILY": "Daily",
    "FORTNIGHTLY": "Fortnightly",
    "HALF YEARLY": "Half-Yearly",
    "HALF-YEARLY": "Half-Yearly"
}

def parse_scheme_category(category):
    if "(" in category:
        scheme_type, rest = category.split("(", 1)
        rest = rest.rstrip(")")
        if "-" in rest:
            category_part, sub_category = rest.split("-", 1)
            category_part = category_part.strip()
            sub_category = sub_category.strip()
        else:
            category_part = rest.strip()
            sub_category = category_part
    else:
        scheme_type = category
        category_part = ""
        sub_category = ""
    return scheme_type, category_part, sub_category

def parse_scheme_name(scheme_name):
    """
    Parse the scheme name to extract plan, option, IDCW frequency, and cleaned name.
    """
    # Normalize the scheme name
    name = scheme_name.upper()

    # Determine plan
    plan = "Other"
    for key, value in PLAN_TYPES.items():
        if key in name:
            plan = value
            break

    # Determine option
    option = "Other"
    if "GROWTH" in name:
        option = OPTION_TYPES["GROWTH"]
    elif "IDCW" in name or "DIVIDEND OPTION" in name or "DIVIDEND" in name or "INCOME DISTRIBUTION CUM CAPITAL WITHDRAWAL" in name or "INCOME CUM DISTRIBUTION CAPITAL WITHDRAWAL" in name:
        option = OPTION_TYPES["IDCW"]
    elif "BONUS" in name:
        option = OPTION_TYPES["BONUS"]

    # For IDCW, extract frequency
    idcw_frequency = ""
    if option == "IDCW":
        for key, value in IDCW_FREQUENCIES.items():
            if key in name:
                idcw_frequency = value
                break
        if not idcw_frequency:
            idcw_frequency = "Other"

    return plan, option, idcw_frequency

print("📁 Preparing directories...")
os.makedirs("Test", exist_ok=True)
os.makedirs(r"data\scheme_data\v2", exist_ok=True)
print("✅ Directories ready\n")

print("🌐 Fetching NAVAll.txt from AMFI...")
try:
    response = requests.get(URL, timeout=20)
    response.raise_for_status()
    print("✅ Download completed\n")

    text = response.text

    current_amc = ""
    current_category = ""
    rows = []

    print("📖 Parsing NAV data and analyzing schemes...\n")

    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()

        if not line:
            continue

        parts = line.split(";")

        # ---------- AMC NAME OR CATEGORY ----------
        if len(parts) == 1 and not parts[0].isdigit():
            line_text = parts[0].strip()
            if "Schemes" in line_text:
                current_category = line_text
                print(f"📂 Category Detected: {current_category}")
            else:
                current_amc = line_text
                print(f"🏢 AMC Detected: {current_amc}")
            continue

        # ---------- SCHEME DATA ----------
        if len(parts) >= 6 and parts[0].isdigit():
            scheme_code = parts[0].strip()
            isin = parts[1].strip() or parts[2].strip()
            scheme_name = parts[3].strip()
            nav = parts[4].strip()
            nav_date = parts[5].strip()

            scheme_type, category, sub_category = parse_scheme_category(current_category)

            # Remove "Scheme" from Category
            category = category.replace(" Scheme", "")

            # Analyze scheme name
            plan, option, idcw_frequency = parse_scheme_name(scheme_name)

            rows.append({
                "SchemeCode": scheme_code,
                "AMC": current_amc,
                "SchemeName": scheme_name,
                "ISIN": isin,
                "NAV": nav,
                "Date": nav_date,
                "SchemeType": scheme_type,
                "Category": category,
                "SubCategory": sub_category,
                "PlanType": plan,
                "PlanOption": option,
                "IDCW_Frequency": idcw_frequency if option == "IDCW" else ""
            })

            # ✅ Clean one-line output
            print(f"📄 {scheme_name}")

    print(f"\n🧮 Total schemes parsed and analyzed: {len(rows)}")

    # Sort rows by SchemeCode (as integer)
    rows.sort(key=lambda x: int(x['SchemeCode']))

    # ---------- WRITE CSV ----------
    print(f"\n💾 Saving combined analyzed data file → {OUT_FILE}\n")

    try:
        with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=CSV_FIELDNAMES
            )
            writer.writeheader()

            for row in rows:
                writer.writerow(row)

        print(f"🎉 Successfully saved {len(rows)} schemes")
        print(f"📦 {OUT_FILE} is ready for use ✅")
    except IOError as e:
        print(f"❌ Error writing CSV: {e}")
        exit(1)

except requests.RequestException as e:
    print(f"❌ Error fetching data: {e}")
    print("Skipping CSV file creation due to link failure.")
