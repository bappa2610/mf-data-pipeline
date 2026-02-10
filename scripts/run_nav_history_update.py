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
    print("🚀 Starting NAV pipeline...")

    # Run fetch_nav_history.py
    fetch_script = "scripts/Export_NAV_history/fetch_nav_history.py"
    print(f"📥 Running {fetch_script}...")
    run_script(fetch_script)

    # Run export_nav_year.py
    export_script = "scripts/Export_NAV_history/export_nav_year.py"
    print(f"📦 Running {export_script}...")
    run_script(export_script)

    print("🎉 NAV pipeline completed successfully!")

if __name__ == "__main__":
    main()
