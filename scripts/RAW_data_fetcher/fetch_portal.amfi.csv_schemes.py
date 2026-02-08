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
        response = requests.get(URL, headers=headers, timeout=30, stream=True)
        response.raise_for_status()

        # Get total size for progress bar
        total_size = int(response.headers.get('content-length', 0))
        content = b''
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
                    pbar.update(len(chunk))

        response_text = content.decode('utf-8')

        # Parse CSV and remove empty rows
        csv_reader = csv.reader(io.StringIO(response_text))
        cleaned_rows = []
        row_count = 0
        for row in tqdm(csv_reader, desc="Processing rows"):
            row_count += 1
            # Check if row is not empty (all fields are not empty after stripping)
            if any(field.strip() for field in row):
                cleaned_rows.append(row)

        # Write cleaned data to CSV file
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(cleaned_rows)
        print(f"Data fetched and saved successfully. Total rows: {len(cleaned_rows)}")

    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        print("Skipping CSV file creation due to link failure.")

if __name__ == "__main__":
    fetch_and_save_csv()
