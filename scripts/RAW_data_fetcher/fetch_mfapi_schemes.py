import requests
import csv
import concurrent.futures
import os
import time
from tqdm import tqdm

# API URLs
BASE_URL = "https://api.mfapi.in/mf"
OUTPUT_FILE = "data/scheme_data/RAW_data/mfapi_schemes.csv"
MAX_WORKERS = 8
MAX_RETRIES = 5

def fetch_all_schemes():
    """Fetch all schemes from the API."""
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        schemes = response.json()
        return schemes
    except requests.RequestException as e:
        print(f"❌ Error fetching schemes: {e}")
        return []

def fetch_scheme_details(scheme):
    """Fetch details for a specific scheme with retries."""
    scheme_code = scheme.get("schemeCode")
    scheme_name = scheme.get("schemeName")
    if not scheme_code or not scheme_name:
        return None

    url = f"{BASE_URL}/{scheme_code}"
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            details = response.json()
            meta = details.get("meta", {})
            return {
                "SchemeCode": scheme_code,
                "AMC": meta.get("fund_house", ""),
                "SchemeName": scheme_name,
                "ISIN": meta.get("isin_growth", ""),
                "ISINdivReinvestment": meta.get("isin_div_reinvestment", ""),
                "SchemeType": meta.get("scheme_type", ""),
                "Category": meta.get("scheme_category", "")
            }
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)  # Wait before retry
            else:
                print(f"❌ Failed to fetch scheme {scheme_code} after {MAX_RETRIES} attempts.")
                return {"SchemeCode": scheme_code, "error": True}

    return None

def main():
    schemes = fetch_all_schemes()
    if not schemes:
        return

    data = []
    failed_schemes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_scheme_details, scheme) for scheme in schemes]
        with tqdm(total=len(schemes), desc="Processing schemes") as pbar:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    if result.get("error"):
                        failed_schemes.append(result["SchemeCode"])
                    else:
                        data.append(result)
                pbar.update(1)

    data.sort(key=lambda x: int(x['SchemeCode']))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["SchemeCode", "AMC", "SchemeName", "ISIN", "ISINdivReinvestment", "SchemeType", "Category"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data fetched and saved successfully. Total schemes: {len(data)}, Failed: {len(failed_schemes)}")

if __name__ == "__main__":
    main()
