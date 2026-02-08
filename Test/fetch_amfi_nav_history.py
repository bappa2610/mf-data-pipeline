import json
import requests
import csv
import io
import os
from datetime import datetime
from tqdm import tqdm

# Load config
config_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

start_date = config['start_date']
end_date = config['end_date']

# Construct URL
url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&frmdt={start_date}&todt={end_date}"

print(f"Fetching NAV history from {start_date} to {end_date}...")

# Fetch data with streaming and retries
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
max_retries = 3
for attempt in range(max_retries):
    try:
        response = requests.get(url, headers=headers, timeout=120, stream=True)
        response.raise_for_status()
        break
    except requests.exceptions.RequestException as e:
        if attempt == max_retries - 1:
            raise e
        print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
        import time
        time.sleep(5)

# Get total size for progress bar
total_size = int(response.headers.get('content-length', 0))
content = b''
with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            content += chunk
            pbar.update(len(chunk))

response_text = content.decode('utf-8')

# Debug: print response info
print(f"Response length: {len(response_text)} characters")
print("First 500 characters of response:")
print(response_text[:500])

# Parse CSV and remove empty rows
csv_reader = csv.DictReader(io.StringIO(response_text), delimiter=';')
nav_data = {}
for row in csv_reader:
    scheme_code = row.get('Scheme Code')
    if not scheme_code:
        continue
    date_str = row.get('Date')
    nav = row.get('Net Asset Value')
    if not date_str or not nav:
        continue
    # Convert date to ISO format
    try:
        date_obj = datetime.strptime(date_str, '%d-%b-%Y')
        iso_date = date_obj.strftime('%Y-%m-%d')
    except ValueError:
        continue  # Skip invalid dates
    if scheme_code not in nav_data:
        nav_data[scheme_code] = []
    nav_data[scheme_code].append({'Date': iso_date, 'NAV': nav})

# Create nav_history directory
nav_history_dir = os.path.join(os.path.dirname(__file__), 'nav_history')
os.makedirs(nav_history_dir, exist_ok=True)

# Write individual CSV files, appending new data only
total_schemes = len(nav_data)
with tqdm(total=total_schemes, desc="Processing schemes") as pbar:
    for scheme_code, rows in nav_data.items():
        filepath = os.path.join(nav_history_dir, f'{scheme_code}.csv')
        existing_dates = set()
        if os.path.exists(filepath):
            with open(filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_dates = {row['Date'] for row in reader if row.get('Date')}

        new_rows = [row for row in rows if row['Date'] not in existing_dates]

        if new_rows:
            write_header = not os.path.exists(filepath)
            with open(filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['Date', 'NAV'])
                if write_header:
                    writer.writeheader()
                writer.writerows(new_rows)
        pbar.update(1)

print(f"Extracted NAV history for {len(nav_data)} schemes into nav_history/ folder.")
