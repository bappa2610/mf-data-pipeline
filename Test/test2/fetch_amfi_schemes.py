import requests
import csv
import os

# URL for downloading scheme data
URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0&utm"
OUTPUT_FILE = "mf_schemes_data.csv"

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

def main():
    print("Step 1: Fetching scheme data from AMFI...")
    try:
        response = requests.get(URL, timeout=20)
        response.raise_for_status()
        print("✅ Download completed")
    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        return

    text = response.text

    current_amc = ""
    current_category = ""
    rows = []

    print("Step 2: Parsing scheme data...")

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

            # Normalize ETF subcategories
            if "Gold ETF" in sub_category or "Other ETFs" in sub_category or "ETF" in sub_category:
                sub_category = "ETF"

            # Analyze scheme name
            plan, option, idcw_frequency = parse_scheme_name(scheme_name)

            rows.append({
                "SchemeCode": scheme_code,
                "AMC": current_amc,
                "SchemeName": scheme_name,
                "ISIN": isin,
                "SchemeType": scheme_type,
                "Category": category
            })

    print(f"✅ Total schemes parsed: {len(rows)}")

    print("Step 3: Sorting data by SchemeCode...")
    rows.sort(key=lambda x: int(x['SchemeCode']))
    print("✅ Data sorted.")

    print("Step 4: Writing data to CSV...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["SchemeCode", "AMC", "SchemeName", "ISIN", "SchemeType", "Category"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"🎉 Data saved to {OUTPUT_FILE}. Total schemes: {len(rows)}")

if __name__ == "__main__":
    main()
