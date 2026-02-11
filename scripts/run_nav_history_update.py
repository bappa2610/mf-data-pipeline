import subprocess
import sys
import os

# List of scripts to run in sequence. Add, remove, or reorder as needed.
SCRIPTS_TO_RUN = [
    "scripts/Export_NAV_history/fetch_nav_history.py",
    "scripts/Export_NAV_history/export_nav_year.py"
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
    print("🚀 Starting NAV History Update...")

    for script in SCRIPTS_TO_RUN:
        print(f"📥 Running {script}...")
        run_script(script)

    print("🎉 NAV History Update completed successfully!")

if __name__ == "__main__":
    main()
