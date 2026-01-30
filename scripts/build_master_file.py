import os
import sys
import subprocess

# Backwards-compat wrapper for legacy callers
print("⚠️ `scripts/build_master_file.py` is deprecated. Use `scripts/merge_scheme_metadata.py` instead.")
print("🛠 Invoking `scripts/merge_scheme_metadata.py` to maintain compatibility...")

script_dir = os.path.dirname(__file__)
new_script = os.path.join(script_dir, "merge_scheme_metadata.py")
try:
    subprocess.run([sys.executable, new_script] + sys.argv[1:], check=True)
except Exception as e:
    print("❌ Error while running the new script:", e)
    sys.exit(1)

