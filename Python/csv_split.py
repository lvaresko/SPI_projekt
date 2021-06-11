import pandas as pd

# Import CSV BeijingHousingPrice file
CSV_FILE_PATH = r"C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned.csv"
df = pd.read_csv(CSV_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1')

# Get number of records in CSV
records_num = df.shape[0]
print("Number of rows ", records_num)

# Split CSV in ratio 80:20
segment_1 = int(records_num * 0.8)
print('Segment_1 size: ', segment_1)
segment_2 = int(records_num * 0.2)
print('Segment_2 size: ', segment_2)


df_1 = pd.read_csv(CSV_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1', nrows = segment_1)
print("CSV_1 size: ", df_1.shape[0])
df_2 = pd.read_csv(CSV_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1', skiprows = range(1, segment_1 + 1))
print("CSV_2 size: ", df_2.shape[0])

# Export created dataframes to CSV
df_1.to_csv(r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned_1.csv', index=False)
df_2.to_csv(r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned_2.csv', index=False)