import requests
import csv
import concurrent.futures
import os
import time

# API URLs
BASE_URL = "https://api.mfapi.in/mf"
OUTPUT_FILE = "schemes.csv"
MAX_WORKERS = 5
MAX_RETRIES = 5

def fetch_all_schemes():
    """Fetch all schemes from the API."""
    print("Step 1: Fetching list of all schemes...")
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        schemes = response.json()
        print(f"✅ Fetched {len(schemes)} schemes.")
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
                return {"SchemeCode": scheme_code, "error": True}

    return None

def main():
    schemes = fetch_all_schemes()
    if not schemes:
        print("No schemes fetched. Exiting.")
        return

    print(f"Step 2: Processing {len(schemes)} schemes with {MAX_WORKERS} workers...")

    data = []
    failed_schemes = []
    processed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_scheme_details, scheme) for scheme in schemes]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                if result.get("error"):
                    failed_schemes.append(result["SchemeCode"])
                else:
                    data.append(result)
            processed += 1
            if processed % 100 == 0:
                print(f"📊 Processed {processed}/{len(schemes)} schemes...")

    print(f"✅ All schemes processed. Total successful: {len(data)}, Failed: {len(failed_schemes)}")

    if failed_schemes:
        print("❌ Failed schemes:")
        for code in failed_schemes:
            print(f"  - {code}")

    print("Step 3: Sorting data by SchemeCode...")
    data.sort(key=lambda x: int(x['SchemeCode']))
    print("✅ Data sorted.")

    print("Step 4: Writing data to CSV...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["SchemeCode", "AMC", "SchemeName", "ISIN", "ISINdivReinvestment", "SchemeType", "Category"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"🎉 Data saved to {OUTPUT_FILE}. Total schemes: {len(data)}")

if __name__ == "__main__":
    main()
