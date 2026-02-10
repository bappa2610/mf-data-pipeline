import json
import os
import concurrent.futures
from tqdm import tqdm

# ================= CONFIG =================
SOURCE_DIR = "data/scheme_data/RAW_data/all_funds"
OUTPUT_FILE = "data/scheme_data/RAW_data/amfi_master_schemes.json"
MAX_WORKERS = 8
# ==========================================

def process_file(filename):
    """Process a single JSON file and return scheme_code and data."""
    scheme_code = filename[:-5]  # Remove .json
    filepath = os.path.join(SOURCE_DIR, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return scheme_code, data
    except (json.JSONDecodeError, IOError) as e:
        print(f"❌ Error reading {filename}: {e}")
        return scheme_code, None

def main():
    print("📁 Checking source directory...")
    if not os.path.exists(SOURCE_DIR):
        print(f"❌ Source directory {SOURCE_DIR} does not exist.")
        return

    print("📄 Loading all scheme data...")
    json_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.json')]
    master_data = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, filename) for filename in json_files]
        with tqdm(total=len(json_files), desc="Processing schemes") as pbar:
            for future in concurrent.futures.as_completed(futures):
                scheme_code, data = future.result()
                if data is not None:
                    master_data[scheme_code] = data
                pbar.update(1)

    print(f"📊 Total schemes processed: {len(master_data)}")

    print("💾 Saving master data...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(master_data, f, indent=4)

    print(f"✅ Master scheme data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
