import csv
import os

def load_lookup(file_path, key_col, type_col, category_col):
    lookup = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row[key_col]
            lookup[key] = {
                'SchemeType': row[type_col],
                'Category': row[category_col]
            }
    return lookup

def main():
    base_dir = os.path.dirname(__file__)
    mfapi_file = os.path.join(base_dir, 'filtered_mfapi_schemes.csv')
    portal_file = os.path.join(base_dir, 'modified_portal.amfi.csv_schemes.csv')
    amfi_file = os.path.join(base_dir, 'amfi_nav_all.csv')
    output_file = os.path.join(base_dir, 'updated_filtered_mfapi_schemes.csv')

    # Load lookups
    portal_lookup = load_lookup(portal_file, 'SchemeCode', 'SchemeType', 'Category')
    amfi_lookup = load_lookup(amfi_file, 'SchemeCode', 'SchemeType', 'Category')

    # Process mfapi file
    updated_rows = []
    with open(mfapi_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            scheme_code = row['SchemeCode']
            if scheme_code in portal_lookup:
                row['SchemeType'] = portal_lookup[scheme_code]['SchemeType']
                row['Category'] = portal_lookup[scheme_code]['Category']
            elif scheme_code in amfi_lookup:
                row['SchemeType'] = amfi_lookup[scheme_code]['SchemeType']
                row['Category'] = amfi_lookup[scheme_code]['Category']
            # If not found, keep original
            updated_rows.append(row)

    # Write to new CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    print(f"Updated CSV created: {output_file}")

if __name__ == '__main__':
    main()
