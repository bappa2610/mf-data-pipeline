import pandas as pd
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration
max_workers = 5
max_retries = 5

# Path to the CSV file
csv_path = 'data/scheme_data/MF_data/amfi_mf_analyzed_schemes.csv'

# Output folder
output_folder = 'data/NAV/latest_NAV'

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Read the CSV file
df = pd.read_csv(csv_path)

# Function to fetch NAV with retry
def fetch_nav(scheme_code, max_retries):
    url = f'https://api.mfapi.in/mf/{scheme_code}/latest'
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retry
    return None

# List to hold all NAV data
nav_data = []
successful_fetches = 0
failed_fetches = 0

# Use ThreadPoolExecutor with specified workers
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit tasks
    futures = {executor.submit(fetch_nav, str(row['SchemeCode']), max_retries): row['SchemeCode'] for index, row in df.iterrows()}
    
    # Collect results with progress bar
    for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching NAV"):
        scheme_code = futures[future]
        try:
            data = future.result()
            if data:
                nav_data.append(data)
                successful_fetches += 1
            else:
                failed_fetches += 1
        except Exception:
            failed_fetches += 1

# Create DataFrame and save to CSV
if nav_data:
    nav_df = pd.DataFrame(nav_data)
    csv_file_path = os.path.join(output_folder, 'latest_nav.csv')
    nav_df.to_csv(csv_file_path, index=False)

# Final report
total_schemes = len(df)
print(f"\nFinal Report:")
print(f"Total schemes processed: {total_schemes}")
print(f"Successful fetches: {successful_fetches}")
print(f"Failed fetches: {failed_fetches}")
if nav_data:
    print(f"Data saved to: {csv_file_path}")
else:
    print("No data fetched.")
print('Script completed.')
