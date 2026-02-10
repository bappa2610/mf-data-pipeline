import subprocess
import sys
import os

def run_script(script_path):
    """Run a Python script and check for errors."""
    try:
        result = subprocess.run([sys.executable, script_path], check=True, cwd=os.getcwd())
        print(f"✅ {script_path} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_path}: {e}")
        sys.exit(1)

def main():
    print("🚀 Starting MFAPI data pipeline...")

    # Run fetch_mfapi_all_funds.py
    fetch_script = "scripts/RAW_data_fetcher/fetch_mfapi_all_funds.py"
    print(f"📥 Running {fetch_script}...")
    run_script(fetch_script)

    # Run create_mfapi_master_data.py
    master_script = "scripts/RAW_data_fetcher/create_mfapi_master_data.py"
    print(f"📦 Running {master_script}...")
    run_script(master_script)

    print("🎉 MFAPI data pipeline completed successfully!")

if __name__ == "__main__":
    main()
