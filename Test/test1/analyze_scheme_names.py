import csv
import re
import os

INPUT_FILE = "Test/test.csv"
OUTPUT_FILE = "Test/analyzed_schemes.csv"

def parse_scheme_name(scheme_name):
    """
    Parse the scheme name to extract plan, option, IDCW frequency, and cleaned name.
    """
    # Normalize the scheme name
    name = scheme_name.upper()

    # Determine plan: Direct, Regular, Retail, Institutional, etc.
    plan = "Other"
    if "DIRECT" in name:
        plan = "Direct"
    elif "REGULAR" in name:
        plan = "Regular"
    elif "RETAIL" in name:
        plan = "Retail"
    elif "INSTITUTIONAL" in name:
        plan = "Institutional"
    elif "PREMIUM" in name:
        plan = "Premium"
    elif "STANDARD" in name:
        plan = "Standard"

    # Determine option: Growth, IDCW, etc.
    if "GROWTH" in name:
        option = "Growth"
    elif "IDCW" in name or "DIVIDEND OPTION" in name or "DIVIDEND" in name or "INCOME DISTRIBUTION CUM CAPITAL WITHDRAWAL" in name or "INCOME CUM DISTRIBUTION CAPITAL WITHDRAWAL" in name:
        option = "IDCW"
    elif "BONUS" in name:
        option = "Bonus"
    else:
        option = "Other"

    # For IDCW, extract frequency
    idcw_frequency = ""
    if option == "IDCW":
        # Look for frequency keywords
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
        else:
            idcw_frequency = "Other"

    # Clean the scheme name: remove plan, option, and other common parts for a professional name
    cleaned_name = scheme_name
    # Remove plan keywords
    plan_keywords = ["DIRECT", "REGULAR", "RETAIL", "INSTITUTIONAL", "PREMIUM", "STANDARD"]
    for keyword in plan_keywords:
        cleaned_name = re.sub(r'\b' + keyword + r'\b', '', cleaned_name, flags=re.IGNORECASE)

    # Remove option keywords
    option_keywords = ["GROWTH", "IDCW", "BONUS", "MONTHLY", "QUARTERLY", "ANNUAL", "YEARLY", "WEEKLY", "DAILY", "FORTNIGHTLY", "HALF YEARLY", "HALF-YEARLY", "DIVIDEND OPTION", "DIVIDEND", "INCOME DISTRIBUTION CUM CAPITAL WITHDRAWAL", "INCOME CUM DISTRIBUTION CAPITAL WITHDRAWAL"]
    for keyword in option_keywords:
        cleaned_name = re.sub(r'\b' + keyword + r'\b', '', cleaned_name, flags=re.IGNORECASE)

    # Remove other common terms that are not essential for fund identification
    other_keywords = ["FUND", "SCHEME", "PLAN", "OPTION", "REINVESTMENT"]
    for keyword in other_keywords:
        cleaned_name = re.sub(r'\b' + keyword + r'\b', '', cleaned_name, flags=re.IGNORECASE)

    # Clean up extra spaces and dashes
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
    cleaned_name = re.sub(r'\s*-\s*', ' ', cleaned_name).strip()
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
    # Capitalize words for professional appearance
    cleaned_name = ' '.join(word.capitalize() for word in cleaned_name.split())

    return plan, option, idcw_frequency, cleaned_name

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found.")
        return

    rows = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scheme_name = row["SchemeName"]
            plan, option, idcw_frequency, cleaned_name = parse_scheme_name(scheme_name)

            # Remove "Scheme" from Category
            row["Category"] = row["Category"].replace(" Scheme", "")

            # Add new columns
            row["Plan"] = plan
            row["Option"] = option
            row["IDCW_Frequency"] = idcw_frequency if option == "IDCW" else ""

            rows.append(row)

    # Write to output file
    if rows:
        fieldnames = list(rows[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ Analysis complete. Output saved to {OUTPUT_FILE}")
        print(f"📊 Total schemes analyzed: {len(rows)}")
    else:
        print("❌ No data to process.")

if __name__ == "__main__":
    main()
