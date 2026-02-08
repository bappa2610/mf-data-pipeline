import pandas as pd

print("Reading the CSV file...")
# Read the CSV file
df = pd.read_csv('data/scheme_data/RAW_data/portal.amfi.csv_schemes.csv')
print("CSV file read successfully.")

print("Renaming the malformed last column...")
# Rename the last column which is malformed
df.rename(columns={'ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment': 'ISIN_combined'}, inplace=True)
print("Last column renamed to 'ISIN_combined'.")

print("Splitting the combined ISIN column into two...")
# Split the combined ISIN column into two
df['ISIN'] = df['ISIN_combined'].str[:12]
df['ISINdivReinvestment'] = df['ISIN_combined'].str[12:].str.strip()
print("ISIN column split into 'ISIN' and 'ISINdivReinvestment'.")

print("Renaming columns as per requirements...")
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
print("Columns renamed successfully.")

print("Dropping the combined ISIN column...")
# Drop the combined column
df.drop('ISIN_combined', axis=1, inplace=True)
print("Combined ISIN column dropped.")

print("Reordering columns...")
# Reorder columns
new_order = ['SchemeCode', 'AMC', 'SchemeName', 'ISIN', 'ISINdivReinvestment', 'SchemeType', 'Category', 'SchemeNameCleaned', 'SchemeMinimumAmount', 'LaunchDate', 'ClosureDate']
df = df[new_order]
print("Columns reordered.")

print("Saving the modified DataFrame to a new CSV file...")
# Save the modified DataFrame to a new CSV file
df.to_csv('data/scheme_data/analytics/modified_portal.amfi.csv_schemes.csv', index=False)
print("Modified CSV saved as 'data/scheme_data/analytics/modified_portal.amfi.csv_schemes.csv'.")
