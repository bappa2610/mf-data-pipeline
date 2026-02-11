import subprocess
import sys
import os

# List of scripts to run in sequence. Add, remove, or reorder as needed.
SCRIPTS_TO_RUN = [
    "scripts/RAW_data_fetcher/fetch_mfapi_all_funds.py",
]

def run_script(script_path):
    """Run a Python script and check for errors."""
    try:
        result = subprocess.run([sys.executable, script_path], check=True, cwd=os.getcwd())
        print(f"✅ {script_path} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_path}: {e}")
        sys.exit(1)

def main():
    print("🚀 Starting MFAPI data update...")

    for script in SCRIPTS_TO_RUN:
        print(f"📥 Running {script}...")
        run_script(script)

    print("🎉 MFAPI data update completed successfully!")

if __name__ == "__main__":
    main()
