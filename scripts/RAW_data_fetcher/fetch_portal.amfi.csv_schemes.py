import requests
import csv
import io
from tqdm import tqdm

# URL to fetch the CSV data
URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0"
OUTPUT_FILE = "data/scheme_data/RAW_data/portal.amfi.csv_schemes.csv"

def fetch_and_save_csv():
    """Fetch CSV data from the URL, remove empty rows, and save to file."""
    print("🌐 Step 1: Fetching CSV data from AMFI portal...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(URL, headers=headers, timeout=30)
        response.raise_for_status()
        print("✅ Step 2: Data fetched successfully.")
        print(f"Response content preview: {response.text[:1000]}...")

        print("🔄 Step 3: Parsing CSV data...")
        # Parse CSV and remove empty rows
        csv_reader = csv.reader(io.StringIO(response.text))
        cleaned_rows = []
        row_count = 0
        for row in tqdm(csv_reader, desc="Processing rows"):
            row_count += 1
            # Check if row is not empty (all fields are not empty after stripping)
            if any(field.strip() for field in row):
                cleaned_rows.append(row)

        print(f"🧮 Step 5: Total rows processed: {row_count}. Cleaned rows: {len(cleaned_rows)}")

        print("💾 Step 6: Writing cleaned data to CSV file...")
        # Write cleaned data to CSV file
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cleaned_rows)
        print(f"🎉 Step 7: Cleaned CSV data saved to {OUTPUT_FILE}.")

    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        print("Skipping CSV file creation due to link failure.")

if __name__ == "__main__":
    fetch_and_save_csv()
