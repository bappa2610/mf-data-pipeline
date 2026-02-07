import csv
import re
import os

# Configuration Constants
INPUT_FILE = "../../data/scheme_data/RAW_data/amfi_nav_all.csv"
OUTPUT_FILE = "amfi_mf_analyzed_schemes.csv"
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
    Parse the scheme name to extract plan, option, IDCW frequency.
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
        # Additional patterns for frequency
        if not idcw_frequency:
            if "REINVESTMENT" in name:
                idcw_frequency = "Reinvestment"
            elif "MONTHLY" in name:
                idcw_frequency = "Monthly"
            elif "QUARTERLY" in name:
                idcw_frequency = "Quarterly"
            elif "ANNUAL" in name or "YEARLY" in name:
                idcw_frequency = "Annual"
            elif "WEEKLY" in name:
                idcw_frequency = "Weekly"
            elif "DAILY" in name:
                idcw_frequency = "Daily"
            elif "FORTNIGHTLY" in name:
                idcw_frequency = "Fortnightly"
            elif "HALF YEARLY" in name or "HALF-YEARLY" in name:
                idcw_frequency = "Half-Yearly"

    return plan, option, idcw_frequency

def main():
    rows = []

    print(f"📖 Reading input file → {INPUT_FILE}")

    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                scheme_code = row["SchemeCode"]
                amc = row["AMC"]
                scheme_name = row["SchemeName"]
                isin = row["ISIN"]
                nav = row["NAV"]
                date = row["NAV date"]  # Map to Date
                scheme_type = row["SchemeType"]
                category = row["Category"]

                # Remove "Scheme" from Category
                category = category.replace(" Scheme", "")

                # Extract SubCategory
                sub_category = ""
                if " - " in category:
                    parts = category.split(" - ", 1)
                    category = parts[0].strip()
                    sub_category = parts[1].strip()

                # Normalize ETF subcategories
                if "Gold ETF" in sub_category or "Other ETFs" in sub_category or "ETF" in sub_category:
                    sub_category = "ETF"

                # Parse scheme name
                plan, option, idcw_frequency = parse_scheme_name(scheme_name)

                analyzed_row = {
                    "SchemeCode": scheme_code,
                    "AMC": amc,
                    "SchemeName": scheme_name,
                    "ISIN": isin,
                    "NAV": nav,
                    "Date": date,
                    "SchemeType": scheme_type,
                    "Category": category,
                    "SubCategory": sub_category,
                    "PlanType": plan,
                    "PlanOption": option,
                    "IDCW_Frequency": idcw_frequency if option == "IDCW" else ""
                }

                rows.append(analyzed_row)

                print(f"📄 Analyzed: {scheme_name}")

    except FileNotFoundError:
        print(f"❌ Error: {INPUT_FILE} not found.")
        return
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    print(f"\n🧮 Total schemes analyzed: {len(rows)}")

    # Sort rows by SchemeCode (as integer)
    rows.sort(key=lambda x: int(x['SchemeCode']))

    # ---------- WRITE CSV ----------
    print(f"\n💾 Saving analyzed data file → {OUTPUT_FILE}\n")

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        print(f"🎉 Successfully saved {len(rows)} schemes")
        print(f"📦 {OUTPUT_FILE} is ready for use ✅")
    except IOError as e:
        print(f"❌ Error writing CSV: {e}")

if __name__ == "__main__":
    main()
