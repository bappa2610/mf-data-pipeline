import csv

# Specified scheme types to match
specified_types = {"Open Ended Schemes", "Close Ended Schemes", "Interval Fund Schemes"}

# Read the CSV file and filter rows
matched_rows = []
unmatched_rows = []

with open('data/scheme_data/RAW_data/mfapi_schemes.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['SchemeType'] in specified_types:
            matched_rows.append(row)
        else:
            unmatched_rows.append(row)

# Create CSV for matched scheme types
if matched_rows:
    filename_matched = 'data/scheme_data/analytics/mfapi_schemes_matched.csv'
    with open(filename_matched, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=matched_rows[0].keys())
        writer.writeheader()
        writer.writerows(matched_rows)
    print(f"Created {filename_matched} with {len(matched_rows)} matched rows")
else:
    print("No matched scheme types found.")

# Create CSV for unmatched scheme types
if unmatched_rows:
    filename_unmatched = 'data/scheme_data/analytics/mfapi_schemes_unmatched.csv'
    with open(filename_unmatched, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=unmatched_rows[0].keys())
        writer.writeheader()
        writer.writerows(unmatched_rows)
    print(f"Created {filename_unmatched} with {len(unmatched_rows)} unmatched rows")
else:
    print("No unmatched scheme types found.")
