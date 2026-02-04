import argparse
import csv
import json
import os
from datetime import datetime

NAV_DIR = "data/nav_history"
OUTPUT_FILE = "data/nav_history_all.csv"
META_FILE = "data/nav_history_all.meta.json"

FIELDNAMES = ["SchemeCode", "Date", "NAV"]


def load_meta(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_meta_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def parse_date(d):
    # Expect ISO YYYY-MM-DD
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return None


def full_rebuild(nav_dir, output_file, meta_file, dry_run=False):
    print("🔁 Performing full rebuild of combined NAV history...")

    meta = {}
    rows_written = 0

    if dry_run:
        print("--dry-run: no files will be written")

    scheme_files = sorted(
        f for f in os.listdir(nav_dir) if f.endswith(".csv")
    )

    if not dry_run:
        os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
        out_f = open(output_file, "w", newline="", encoding="utf-8")
        writer = csv.writer(out_f)
        writer.writerow(FIELDNAMES)
    else:
        writer = None

    for fname in scheme_files:
        scheme_code = os.path.splitext(fname)[0]
        path = os.path.join(nav_dir, fname)

        max_date = None
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                date_str = r.get("Date")
                nav = r.get("NAV")
                if not date_str or not nav:
                    continue
                date_obj = parse_date(date_str)
                if not date_obj:
                    continue
                if writer:
                    writer.writerow([scheme_code, date_str, nav])
                rows_written += 1
                if not max_date or date_str > max_date:
                    max_date = date_str

        if max_date:
            meta[scheme_code] = max_date

    if writer:
        out_f.close()

    if not dry_run:
        save_meta_atomic(meta_file, meta)

    print(f"✅ Full rebuild complete. Rows written: {rows_written}")
    return rows_written


def incremental_update(nav_dir, output_file, meta_file, dry_run=False):
    print("⚙️ Performing incremental update using meta index...")

    meta = load_meta(meta_file)
    rows_appended = 0

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    need_header = not os.path.exists(output_file)

    scheme_files = sorted(
        f for f in os.listdir(nav_dir) if f.endswith(".csv")
    )

    for fname in scheme_files:
        scheme_code = os.path.splitext(fname)[0]
        path = os.path.join(nav_dir, fname)

        last_known = meta.get(scheme_code)  # ISO string

        to_write = []
        max_date = last_known

        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                date_str = r.get("Date")
                nav = r.get("NAV")
                if not date_str or not nav:
                    continue
                if last_known and date_str <= last_known:
                    continue
                date_obj = parse_date(date_str)
                if not date_obj:
                    continue
                to_write.append((scheme_code, date_str, nav))
                if not max_date or date_str > max_date:
                    max_date = date_str

        if not to_write:
            print(f"- {scheme_code} → up to date")
            continue

        to_write.sort(key=lambda x: x[1])

        print(f"- {scheme_code} → +{len(to_write)} rows")

        if not dry_run:
            mode = "a"
            with open(output_file, mode, newline="", encoding="utf-8") as out_f:
                writer = csv.writer(out_f)
                if need_header:
                    writer.writerow(FIELDNAMES)
                    need_header = False
                writer.writerows(to_write)

            # update meta for this scheme and persist after each successful write
            if max_date:
                meta[scheme_code] = max_date
                save_meta_atomic(meta_file, meta)

        rows_appended += len(to_write)

    print(f"✅ Incremental update complete. Rows appended: {rows_appended}")
    return rows_appended


def main():
    parser = argparse.ArgumentParser(description="Incremental merge of NAV history CSVs")
    parser.add_argument("--rebuild", action="store_true", help="Do a full rebuild instead of incremental append")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output merged CSV path")
    parser.add_argument("--meta", default=META_FILE, help="Meta JSON path to track per-scheme last dates")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change but do not write files")

    args = parser.parse_args()

    if not os.path.exists(NAV_DIR):
        print("⚠️ NAV directory not found:", NAV_DIR)
        return

    if args.rebuild or not os.path.exists(args.output) or not os.path.exists(args.meta):
        full_rebuild(NAV_DIR, args.output, args.meta, dry_run=args.dry_run)
    else:
        incremental_update(NAV_DIR, args.output, args.meta, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
