# Imports
import pandas as pd
import numpy as np

# Import CSV BeijingHousingPrice file
CSV_FILE_PATH = r"C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_original.csv"
df = pd.read_csv(CSV_FILE_PATH, delimiter=',', parse_dates=["tradeTime"], encoding='ISO-8859–1')
print("CSV size: ", df.shape)


# Rename columns with confusing/wrong name
df = df.rename({'tradeTime': 'sale_date', 'price' : 'avg_square_price', 'livingRoom': 'bedroom', 'drawingRoom': 'hall'}, axis=1)
print(df.columns.values)

# Remove rows with Null/NaN values
df = df.dropna(axis=0, subset=['buildingType', 'elevator', 'fiveYearsProperty', ])
print(df.isna().sum())

# Remove rows with strange value
df = df[df.constructionTime != 'Î´Öª']
print(df['constructionTime'].value_counts())

# DOM -> replace null values with 1
df.DOM.replace(np.NaN, 1, inplace=True)
# reset index
df = df.reset_index(drop=True) 

# Replace values
df['buildingStructure'] = df['buildingStructure'].replace([1,2,3,4,5,6],['Unknown','Mixed', 'Brick/Wood', 'Brick/Concrete', 'Steel', 'Steel/Concrete'])
print(df['buildingStructure'].value_counts())

df['buildingType'] = df['buildingType'].replace([1.0,2.0,3.0,4.0],['Tower', 'Bungalow', 'Plate/Tower', 'Plate'])
print(df['buildingType'].value_counts())

df['renovationCondition'] = df['renovationCondition'].replace([1,2,3,4],['Other', 'Rough', 'Simplicity', 'Refined decoration'])
print(df['renovationCondition'].value_counts())

df['elevator'] = df['elevator'].replace([0,1],['Absent','Present'])
print(df['elevator'].value_counts())

df['fiveYearsProperty'] = df['fiveYearsProperty'].replace([0,1],['Ownership>5y','Ownership<5y'])
print(df['fiveYearsProperty'].value_counts())

df['district'] = df['district'].replace([1,2,3,4,5,6,7,8,9,10,11,12,13],['Dongcheng District','Xicheng District','Chaoyang District','Fengtai District','Shijingshan District','Haidian District','Mentougou District','Fangshan District','Tongzhou District','Shunyi District','Changping District','Daxing District','Huairou District'])
print(df['district'].value_counts())

# Type conversion
df = df.astype({'constructionTime':'int64','bedroom':'int64','bathRoom':'int64','avg_square_price':'float64'})


df.info()
print(df.shape)

# Export to CSV
df.to_csv(r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned.csv', index=False)