import requests
import csv
import io
from tqdm import tqdm

# URL to fetch the NAV data
URL = "https://www.amfiindia.com/spages/NAVAll.txt"
OUTPUT_FILE = "data/scheme_data/RAW_data/amfi_nav_all.csv"

def fetch_and_save_nav_csv():
    """Fetch NAV data from the URL, parse as tab-separated, and save to CSV file."""
    print("🌐 Step 1: Fetching NAV data from AMFI...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(URL, headers=headers, timeout=30)
        response.raise_for_status()
        print("✅ Step 2: Data fetched successfully.")
        print(f"Response content preview: {response.text[:1000]}...")

        print("🔄 Step 3: Parsing CSV data...")
        # Parse as semicolon-separated CSV and remove empty rows
        csv_reader = csv.reader(io.StringIO(response.text), delimiter=';')
        cleaned_rows = []
        header = None
        current_category = ""
        current_amc = ""
        row_count = 0
        with tqdm(desc="Processing schemes", unit="schemes") as pbar:
            for row in csv_reader:
                row_count += 1
                if not header:
                    header = row
                    pbar.write(f"📋 Step 4: Processed header row. Columns: {header}")
                    # Map to available columns
                    col_indices = {
                        'SchemeCode': header.index('Scheme Code'),
                        'SchemeName': header.index('Scheme Name'),
                        'ISIN': header.index('ISIN Div Payout/ ISIN Growth'),
                        'ISINdivReinvestment': header.index('ISIN Div Reinvestment'),
                        'NAV': header.index('Net Asset Value'),
                        'NAV date': header.index('Date')
                    }
                    # Write new header
                    cleaned_rows.append(['SchemeCode', 'AMC', 'SchemeName', 'ISIN', 'ISINdivReinvestment', 'SchemeType', 'Category', 'NAV', 'NAV date'])
                    pbar.write("📝 Step 5: New header added to cleaned rows.")
                else:
                    # Check if it's a category line (single field, contains "Schemes")
                    if len(row) == 1 and row[0].strip() and "Schemes" in row[0].strip():
                        current_category = row[0].strip()
                        pbar.write(f"🏷️ Step 6: Detected category - {current_category}")
                    # Check if it's an AMC line (single field, not digit, and does not contain "Schemes")
                    elif len(row) == 1 and row[0].strip() and not row[0].strip().isdigit() and "Schemes" not in row[0].strip():
                        current_amc = row[0].strip()
                        pbar.write(f"🏢 Step 7: Detected AMC - {current_amc}")
                    # Check if row is data row: starts with digit, has enough columns
                    elif len(row) >= len(col_indices) and row[0].strip().isdigit():
                        # Split current_category into SchemeType and Category
                        if '(' in current_category and ')' in current_category:
                            scheme_type = current_category.split('(')[0].strip()
                            category = current_category.split('(')[1].split(')')[0].strip()
                        else:
                            scheme_type = current_category
                            category = ""
                        # Clean ISIN and ISINdivReinvestment
                        isin = row[col_indices['ISIN']].replace('-', '') if row[col_indices['ISIN']] != '-' else ''
                        isin_div_reinvestment = row[col_indices['ISINdivReinvestment']].replace('-', '') if row[col_indices['ISINdivReinvestment']] != '-' else ''
                        # Select columns
                        selected_row = [
                            row[col_indices['SchemeCode']].strip(),
                            current_amc.strip(),
                            row[col_indices['SchemeName']].strip(),
                            isin.strip(),
                            isin_div_reinvestment.strip(),
                            scheme_type.strip(),
                            category.strip(),
                            row[col_indices['NAV']].strip(),
                            row[col_indices['NAV date']].strip()
                        ]
                        cleaned_rows.append(selected_row)
                        pbar.update(1)

        print(f"🧮 Step 9: Total rows processed: {row_count}. Cleaned rows: {len(cleaned_rows)}")

        print("🔄 Step 10: Sorting data rows by SchemeCode...")
        # Sort data rows by SchemeCode (numerically)
        cleaned_rows[1:] = sorted(cleaned_rows[1:], key=lambda x: int(x[0]))
        print("✅ Step 11: Data sorted successfully.")

        print("💾 Step 12: Writing cleaned data to CSV file...")
        # Write cleaned data to CSV file
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cleaned_rows)
        print(f"🎉 Step 13: NAV data saved to {OUTPUT_FILE}.")

    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        print("Skipping CSV file creation due to link failure.")

if __name__ == "__main__":
    fetch_and_save_nav_csv()
