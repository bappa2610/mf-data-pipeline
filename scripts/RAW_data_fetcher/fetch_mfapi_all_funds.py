import requests
import json
import os
import concurrent.futures
import time
from tqdm import tqdm

# Configuration
BASE_URL = "https://api.mfapi.in/mf"
OUTPUT_DIR = "data/scheme_data/RAW_data/all_funds"
MAX_WORKERS = 5
MAX_RETRIES = 5

def fetch_all_schemes():
    """Fetch list of all schemes."""
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        schemes = response.json()
        return [scheme['schemeCode'] for scheme in schemes if 'schemeCode' in scheme]
    except requests.RequestException as e:
        print(f"❌ Error fetching schemes list: {e}")
        return []

def fetch_scheme_data(scheme_code):
    """Fetch data for a specific scheme with retries."""
    url = f"{BASE_URL}/{scheme_code}"
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
            else:
                print(f"❌ Failed to fetch scheme {scheme_code} after {MAX_RETRIES} attempts: {e}")
                return None
    return None

def process_scheme(scheme_code):
    """Process a single scheme: fetch, check for changes, save if needed."""
    data = fetch_scheme_data(scheme_code)
    if not data:
        return {'status': 'failed', 'scheme_code': scheme_code}

    file_path = os.path.join(OUTPUT_DIR, f"{scheme_code}.json")

    # Check if file exists and data has changed
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            if data == existing_data:
                return {'status': 'unchanged', 'scheme_code': scheme_code}
            else:
                # Data changed, update
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
                return {'status': 'updated', 'scheme_code': scheme_code}
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading existing file for {scheme_code}: {e}")
            # Overwrite if corrupted
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            return {'status': 'updated', 'scheme_code': scheme_code}
    else:
        # New file
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        return {'status': 'new', 'scheme_code': scheme_code}

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    schemes = fetch_all_schemes()
    if not schemes:
        print("No schemes to process.")
        return

    results = {'new': 0, 'updated': 0, 'unchanged': 0, 'failed': 0}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_scheme, scheme) for scheme in schemes]
        with tqdm(total=len(schemes), desc="Processing schemes") as pbar:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results[result['status']] += 1
                pbar.update(1)

    # Summary
    print("\nSummary:")
    print(f"Total schemes processed: {len(schemes)}")
    print(f"New files created: {results['new']}")
    print(f"Files updated: {results['updated']}")
    print(f"Files unchanged: {results['unchanged']}")
    print(f"Failed fetches: {results['failed']}")

if __name__ == "__main__":
    main()
