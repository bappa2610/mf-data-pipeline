import csv
import requests
import time
import os
import sys

CATEGORY_FILE = "data/scheme_categories.csv"
CODE_FILE = "data/scheme_codes.csv"

# ---------- ADAPTIVE SETTINGS ---------- #
BASE_REQUEST_DELAY = 0.12
BASE_CHUNK_DELAY = 6

FIELDNAMES = [
    "SchemeCode",
    "AMC",
    "SchemeType",
    "CategoryRaw",
    "Category",
    "SubCategory"
]

existing = {}
printed_schemes = set()   # 🔹 track printed scheme names

print("📂 Loading existing category data...")

# ---------- LOAD EXISTING DATA ---------- #
if os.path.exists(CATEGORY_FILE):
    with open(CATEGORY_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            existing[row["SchemeCode"]] = row

print(f"✅ Existing schemes loaded: {len(existing)}")

print("📂 Loading scheme codes...")

# ---------- LOAD SCHEME CODES ---------- #
with open(CODE_FILE, newline="", encoding="utf-8") as f:
    codes = list(csv.DictReader(f))

pending_codes = [r for r in codes if r["SchemeCode"] not in existing]

total_pending = len(pending_codes)
print(f"⏳ Pending schemes to process: {total_pending}")

if total_pending == 0:
    print("Nothing to process. Exiting ✅")
    sys.exit(0)

# ---------- ADAPTIVE CHUNK SIZE ---------- #
if total_pending > 8000:
    CHUNK_SIZE = 100
    REQUEST_DELAY = 0.10
elif total_pending > 2000:
    CHUNK_SIZE = 75
    REQUEST_DELAY = 0.12
else:
    CHUNK_SIZE = 50
    REQUEST_DELAY = 0.15

print(
    f"⚙️ Using CHUNK_SIZE={CHUNK_SIZE}, "
    f"REQUEST_DELAY={REQUEST_DELAY}s"
)

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ---------- PROCESS IN CHUNKS ---------- #
for i in range(0, total_pending, CHUNK_SIZE):
    chunk = pending_codes[i:i + CHUNK_SIZE]

    print(
        f"\n📦 Processing chunk {i // CHUNK_SIZE + 1} "
        f"({i + 1}-{i + len(chunk)})"
    )

    for row in chunk:
        scheme_code = row["SchemeCode"]
        scheme_name = row.get("SchemeName", "").strip()

        # ✅ print scheme name only once
        if scheme_name and scheme_name not in printed_schemes:
            print(f"📄 Scheme detected: {scheme_name}")
            printed_schemes.add(scheme_name)

        try:
            r = session.get(
                f"https://api.mfapi.in/mf/{scheme_code}",
                timeout=(5, 10)
            )

            if r.status_code != 200:
                continue

            meta = r.json().get("meta", {})
            if not meta:
                continue

            amc = meta.get("fund_house", "").strip()
            scheme_type = meta.get("scheme_type", "").strip()
            category_raw = meta.get("scheme_category", "").strip()

            if not category_raw:
                continue

            # ---------- CATEGORY SPLIT ---------- #
            cleaned_category = category_raw.replace(" Scheme", "").strip()

            if " - " in category_raw:
                category, sub_category = cleaned_category.split(" - ", 1)
                category = category.strip()
                sub_category = sub_category.strip()
            else:
                category = cleaned_category
                sub_category = ""

            existing[scheme_code] = {
                "SchemeCode": scheme_code,
                "AMC": amc,
                "SchemeType": scheme_type,
                "CategoryRaw": category_raw,
                "Category": category,
                "SubCategory": sub_category
            }

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print("❌ Error:", scheme_code, e)

    # ---------- SAVE AFTER EACH CHUNK ---------- #
    os.makedirs("data", exist_ok=True)

    with open(CATEGORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for v in existing.values():
            writer.writerow(v)

    print("💾 Chunk saved successfully ✅")
    time.sleep(BASE_CHUNK_DELAY)

print("\n🎉 All chunks processed successfully")
print(f"📄 Total unique schemes displayed: {len(printed_schemes)}")
