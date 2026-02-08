import os
import sys

# List of scripts to run in order
scripts = [
    'scripts/RAW_data_fetcher/fetch_amfi_nav_all.py',
    'scripts/RAW_data_fetcher/fetch_portal.amfi.csv_schemes.py',
    'scripts/RAW_data_fetcher/fetch_mfapi_schemes.py',
    'scripts/MF_data_analysis/filter_mfapi_schemes.py',
    'scripts/MF_data_analysis/modify_portal.amfi.csv_schemes.py',
    'scripts/MF_data_analysis/mf_schemes_combiner.py',
    'scripts/Master_data/amfi_MF_data_parser.py'
]

def run_script(script_path):
    """Run a Python script using os.system."""
    print(f"Running {script_path}...")
    exit_code = os.system(f"python {script_path}")
    if exit_code == 0:
        print(f"✅ {script_path} completed successfully.")
        return True
    else:
        print(f"❌ Error running {script_path}: exit code {exit_code}")
        return False

def main():
    print("Starting master scheme data generation flow...")
    # Create necessary directories
    os.makedirs("data/scheme_data/RAW_data", exist_ok=True)
    os.makedirs("data/scheme_data/analytics", exist_ok=True)
    os.makedirs("data/scheme_data/MF_data", exist_ok=True)
    print("✅ Directories created.")
    for script in scripts:
        if not run_script(script):
            print(f"Flow stopped due to error in {script}")
            sys.exit(1)
    print("🎉 All scripts completed successfully. Master scheme data generated.")

if __name__ == "__main__":
    main()
