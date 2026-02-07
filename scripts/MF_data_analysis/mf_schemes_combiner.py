import pandas as pd
import os
import shutil

# File paths
source_file = 'data/scheme_data/RAW_data/amfi_nav_all.csv'
base_file = 'data/scheme_data/analytics/mf_schemes_combined.csv'
mfapi_matched_file = 'data/scheme_data/analytics/mfapi_schemes_matched.csv'
portal_modified_file = 'data/scheme_data/analytics/modified_portal.amfi.csv_schemes.csv'

# Columns to include
required_columns = ['SchemeCode', 'AMC', 'SchemeName', 'ISIN', 'ISINdivReinvestment', 'SchemeType', 'Category']

def combine_schemes():
    print("🔄 Step 1: Copying amfi_nav_all.csv to analytics folder as base file...")
    # Ensure the analytics directory exists
    os.makedirs(os.path.dirname(base_file), exist_ok=True)

    # Read the source file and select required columns
    try:
        df_base = pd.read_csv(source_file)
        df_base = df_base[required_columns]
        df_base.to_csv(base_file, index=False)
        print(f"✅ Step 2: Base file created with {len(df_base)} rows from amfi_nav_all.csv")
    except FileNotFoundError:
        print(f"❌ Error: {source_file} not found.")
        return
    except Exception as e:
        print(f"❌ Error processing base file: {e}")
        return

    print("🔄 Step 3: Reading mfapi_schemes_matched.csv...")
    try:
        df_mfapi = pd.read_csv(mfapi_matched_file)
        df_mfapi = df_mfapi[required_columns]
        print(f"✅ Step 4: Loaded {len(df_mfapi)} rows from mfapi_schemes_matched.csv")
    except FileNotFoundError:
        print(f"❌ Error: {mfapi_matched_file} not found.")
        df_mfapi = pd.DataFrame()
    except Exception as e:
        print(f"❌ Error reading mfapi file: {e}")
        df_mfapi = pd.DataFrame()

    print("🔄 Step 5: Reading modified_portal.amfi.csv_schemes.csv...")
    try:
        df_portal = pd.read_csv(portal_modified_file)
        df_portal = df_portal[required_columns]
        print(f"✅ Step 6: Loaded {len(df_portal)} rows from modified_portal.amfi.csv_schemes.csv")
    except FileNotFoundError:
        print(f"❌ Error: {portal_modified_file} not found.")
        df_portal = pd.DataFrame()
    except Exception as e:
        print(f"❌ Error reading portal file: {e}")
        df_portal = pd.DataFrame()

    print("🔄 Step 7: Combining dataframes...")
    # Start with base
    df_combined = df_base.copy()

    # Add from mfapi where SchemeCode not in base
    if not df_mfapi.empty:
        df_combined = pd.concat([df_combined, df_mfapi[~df_mfapi['SchemeCode'].isin(df_combined['SchemeCode'])]], ignore_index=True)

    # Add from portal where SchemeCode not in combined
    if not df_portal.empty:
        df_combined = pd.concat([df_combined, df_portal[~df_portal['SchemeCode'].isin(df_combined['SchemeCode'])]], ignore_index=True)

    print(f"✅ Step 8: Combined dataframe has {len(df_combined)} rows")

    print("🔄 Step 9: Removing duplicates based on SchemeCode...")
    # Remove duplicates based on SchemeCode, keeping the first occurrence
    df_combined = df_combined.drop_duplicates(subset=['SchemeCode'], keep='first')
    print(f"✅ Step 10: After deduplication, {len(df_combined)} unique rows")

    print("💾 Step 11: Saving to base file...")
    # Save to the base file (overwriting it)
    df_combined.to_csv(base_file, index=False)
    print(f"🎉 Step 12: Successfully saved {len(df_combined)} rows to {base_file}")

if __name__ == "__main__":
    combine_schemes()
