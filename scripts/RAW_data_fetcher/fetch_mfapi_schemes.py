import os
import json
import csv
import concurrent.futures
from tqdm import tqdm

# Local directory
LOCAL_DIR = "data/scheme_data/RAW_data/all_funds"
OUTPUT_FILE = "data/scheme_data/RAW_data/mfapi_schemes.csv"
MAX_WORKERS = 8

def fetch_all_schemes():
    """Fetch all scheme codes from local JSON files."""
    if not os.path.exists(LOCAL_DIR):
        print(f"❌ Directory {LOCAL_DIR} does not exist.")
        return []
    scheme_codes = []
    for filename in os.listdir(LOCAL_DIR):
        if filename.endswith('.json'):
            scheme_code = filename[:-5]  # Remove .json
            scheme_codes.append(scheme_code)
    return scheme_codes

def fetch_scheme_details(scheme_code):
    """Fetch details for a specific scheme from local JSON file."""
    file_path = os.path.join(LOCAL_DIR, f"{scheme_code}.json")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            details = json.load(f)
        meta = details.get("meta", {})
        return {
            "SchemeCode": scheme_code,
            "AMC": meta.get("fund_house", ""),
            "SchemeName": meta.get("scheme_name", ""),
            "ISIN": meta.get("isin_growth", ""),
            "ISINdivReinvestment": meta.get("isin_div_reinvestment", ""),
            "SchemeType": meta.get("scheme_type", ""),
            "Category": meta.get("scheme_category", "")
        }
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"❌ Error reading scheme {scheme_code}: {e}")
        return {"SchemeCode": scheme_code, "error": True}

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
