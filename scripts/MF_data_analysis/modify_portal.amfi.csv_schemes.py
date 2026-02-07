import pandas as pd

print("🔄 Step 1: Reading the CSV file...")
# Read the CSV file
df = pd.read_csv('data/scheme_data/RAW_data/portal.amfi.csv_schemes.csv')
print("✅ Step 2: CSV file read successfully.")

print("📋 Step 3: Original columns:", df.columns.tolist())

print("🔄 Step 4: Renaming the malformed last column...")
# Rename the last column which is malformed
df.rename(columns={'ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment': 'ISIN_combined'}, inplace=True)
print("✅ Step 5: Last column renamed to 'ISIN_combined'.")

print("🔄 Step 6: Splitting the combined ISIN column into two...")
# Split the combined ISIN column into two
df['ISIN'] = df['ISIN_combined'].str[:12]
df['ISINdivReinvestment'] = df['ISIN_combined'].str[12:].str.strip()
print("✅ Step 7: ISIN column split into 'ISIN' and 'ISINdivReinvestment'.")

print("🔄 Step 8: Renaming columns as per requirements...")
# Rename columns as per the requirements
rename_dict = {
    'Code': 'SchemeCode',
    'Scheme Name': 'SchemeNameCleaned',
    'Scheme Type': 'SchemeType',
    'Scheme Category': 'Category',
    'Scheme NAV Name': 'SchemeName',
    'Scheme Minimum Amount': 'SchemeMinimumAmount',
    'Launch Date': 'LaunchDate',
    ' Closure Date': 'ClosureDate'
}
df.rename(columns=rename_dict, inplace=True)
print("✅ Step 9: Columns renamed successfully.")

print("🔄 Step 10: Dropping the combined ISIN column...")
# Drop the combined column
df.drop('ISIN_combined', axis=1, inplace=True)
print("✅ Step 11: Combined ISIN column dropped.")

print("🔄 Step 12: Reordering columns...")
# Reorder columns
new_order = ['SchemeCode', 'AMC', 'SchemeName', 'ISIN', 'ISINdivReinvestment', 'SchemeType', 'Category', 'SchemeNameCleaned', 'SchemeMinimumAmount', 'LaunchDate', 'ClosureDate']
df = df[new_order]
print("✅ Step 13: Columns reordered.")

print("💾 Step 14: Saving the modified DataFrame to a new CSV file...")
# Save the modified DataFrame to a new CSV file
df.to_csv('data/scheme_data/analytics/modified_portal.amfi.csv_schemes.csv', index=False)
print("🎉 Step 15: Modified CSV saved as 'data/scheme_data/analytics/modified_portal.amfi.csv_schemes.csv'.")
