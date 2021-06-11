# Imports
import pandas as pd
import sqlalchemy as db

# Import CSV Sales_Products file
CSV_FILE_PATH = r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned_1.csv'
df = pd.read_csv(CSV_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1')
print("CSV size: ", df.shape)


# Database connection
user = 'root'
passw = 'root123'
host =  'localhost' 
port = 3306 
database = 'beijing_2'

mydb = db.create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
print(mydb)
connection = mydb.connect()


#DDL
district_ddl = "CREATE TABLE beijing_2.district (id INT NOT NULL, name VARCHAR(45) NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC), UNIQUE INDEX name_UNIQUE (name ASC));"
connection.execute(district_ddl)
renovation_condition_ddl = "CREATE TABLE beijing_2.renovation_condition (id INT NOT NULL, name VARCHAR(20) NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC),UNIQUE INDEX name_UNIQUE (name ASC));"
connection.execute(renovation_condition_ddl)
apartment_details_ddl = "CREATE TABLE beijing_2.apartment_details (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, bedroom INT NOT NULL, kitchen INT NOT NULL, bathroom INT NOT NULL, renovation_condition_fk INT NOT NULL, squares FLOAT NOT NULL, five_years_property VARCHAR(12) NOT NULL, UNIQUE INDEX id_UNIQUE (id ASC), CONSTRAINT renovation_condition_id FOREIGN KEY (renovation_condition_fk) REFERENCES pentaho.renovation_condition (id) ON DELETE NO ACTION ON UPDATE CASCADE);"
connection.execute(apartment_details_ddl)
building_structure_ddl = "CREATE TABLE beijing_2.building_structure (id INT NOT NULL, name VARCHAR(15) NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC),UNIQUE INDEX name_UNIQUE (name ASC));"
connection.execute(building_structure_ddl)
building_type_ddl = "CREATE TABLE beijing_2.building_type (id INT NOT NULL, name VARCHAR(15) NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC),UNIQUE INDEX name_UNIQUE (name ASC));"
connection.execute(building_type_ddl)
building_details_ddl = "CREATE TABLE beijing_2.building_details (id INT NOT NULL AUTO_INCREMENT, elevator VARCHAR(8) NOT NULL, building_structure_fk INT NOT NULL, building_type_fk INT NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC), CONSTRAINT building_type_id FOREIGN KEY (building_type_fk) REFERENCES pentaho.building_type (id) ON DELETE NO ACTION ON UPDATE CASCADE, CONSTRAINT building_structure_id FOREIGN KEY (building_structure_fk) REFERENCES pentaho.building_structure (id) ON DELETE NO ACTION ON UPDATE CASCADE);"
connection.execute(building_details_ddl)
sale_ddl = "CREATE TABLE beijing_2.sale (id INT NOT NULL AUTO_INCREMENT, sale_date DATE NOT NULL, sale_price FLOAT NOT NULL, avg_square_price FLOAT NOT NULL, DOM INT NOT NULL, year_built INT NOT NULL, apartment_details_fk INT NOT NULL, building_details_fk INT NOT NULL, district_fk INT NOT NULL, PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC), INDEX apartment_details_id_idx (apartment_details_fk ASC), INDEX building_details_id_idx (building_details_fk ASC), INDEX district_id_idx (district_fk ASC), CONSTRAINT apartment_details_id FOREIGN KEY (apartment_details_fk) REFERENCES pentaho.apartment_details (id) ON DELETE NO ACTION ON UPDATE CASCADE, CONSTRAINT building_details_id FOREIGN KEY (building_details_fk) REFERENCES pentaho.building_details (id) ON DELETE NO ACTION ON UPDATE CASCADE, CONSTRAINT district_id FOREIGN KEY (district_fk) REFERENCES pentaho.district (id) ON DELETE NO ACTION ON UPDATE CASCADE);"
connection.execute(sale_ddl)

#DML
#DISTRICT
district_names = df['district'].unique().tolist()
district_data = pd.DataFrame({'id':list(range(1,len(district_names)+1)),'name':district_names})
district_data.to_sql(con=mydb, name='district', if_exists='append', index=False)

#RENOVATION_CONDITION
renovation_condition_names = df['renovationCondition'].unique().tolist()
renovation_condition_data = pd.DataFrame({'id':list(range(1,len(renovation_condition_names)+1)),'name':renovation_condition_names})
renovation_condition_data.to_sql(con=mydb, name='renovation_condition', if_exists='append', index=False)

#APARTMENT_DETAILS
renovation_condition_fk = []
for i, row in df.iterrows(): 
    renovation_condition = df['renovationCondition'].iloc[i]
    renovation_condition_fk.append(int(renovation_condition_data['id'].loc[renovation_condition_data['name']==renovation_condition]))

apartment_details_data = pd.DataFrame({'bedroom':df['bedroom'], 'kitchen':df['kitchen'], 'bathroom':df['bathRoom'], 'renovation_condition_fk':renovation_condition_fk , 'squares':df['square'], 'five_years_property':df['fiveYearsProperty']})
apartment_details_data = apartment_details_data.drop_duplicates(subset=['bedroom', 'kitchen', 'bathroom', 'renovation_condition_fk', 'squares', 'five_years_property'])
apartment_details_data = apartment_details_data.reset_index(drop=True) 
apartment_details_data.insert(loc=0, column='id', value=list(range(1,apartment_details_data.shape[0]+1)))
apartment_details_data.to_sql(con=mydb, name='apartment_details', if_exists='append', index=False)

#BUILDING_STRUCTURE
building_structure_names = df['buildingStructure'].unique().tolist()
building_structure_data = pd.DataFrame({'id':list(range(1,len(building_structure_names)+1)),'name':building_structure_names})
building_structure_data.to_sql(con=mydb, name='building_structure', if_exists='append', index=False)

#BUILDING_TYPE
building_type_names = df['buildingType'].unique().tolist()
building_type_data = pd.DataFrame({'id':list(range(1,len(building_type_names)+1)),'name':building_type_names})
building_type_data.to_sql(con=mydb, name='building_type', if_exists='append', index=False)

#BUILDING_DETAILS
building_structure_fk, building_type_fk = [], []
for i, row in df.iterrows():
    building_structure = df['buildingStructure'].iloc[i]
    building_structure_fk.append(int(building_structure_data['id'].loc[building_structure_data['name']==building_structure]))

    building_type = df['buildingType'].iloc[i]
    building_type_fk.append(int(building_type_data['id'].loc[building_type_data['name']==building_type]))

building_details_data = pd.DataFrame({'elevator':df['elevator'], 'building_structure_fk':building_structure_fk , 'building_type_fk':building_type_fk})
building_details_data = building_details_data.drop_duplicates(subset=['elevator', 'building_structure_fk', 'building_type_fk'])
building_details_data = building_details_data.reset_index(drop=True) 
building_details_data.insert(loc=0, column='id', value=list(range(1,building_details_data.shape[0]+1)))
building_details_data.to_sql(con=mydb, name='building_details', if_exists='append', index=False)

#SALE
apartment_details_fk, building_details_fk, district_fk = [], [], []
for i, row in df.iterrows():
    bedroom = df['bedroom'].iloc[i]
    kitchen = df['kitchen'].iloc[i]
    bathroom = df['bathRoom'].iloc[i]
    squares = df['square'].iloc[i]
    renovation_cond = renovation_condition_data['id'].loc[renovation_condition_data['name'] == df['renovationCondition'].iloc[i]]
    five_years_property = df['fiveYearsProperty'].iloc[i]
    apartment_details_fk.append(int(apartment_details_data['id'].loc[(apartment_details_data['bedroom']==bedroom) & (apartment_details_data['kitchen']==kitchen) & (apartment_details_data['bathroom']==bathroom) & (apartment_details_data['squares']==squares) & (apartment_details_data['renovation_condition_fk']==renovation_cond.iloc[0]) & (apartment_details_data['five_years_property']==five_years_property)]))

    elevator = df['elevator'].iloc[i]
    b_structure = building_structure_data['id'].loc[building_structure_data['name'] == df['buildingStructure'].iloc[i]]
    b_type = building_type_data['id'].loc[building_type_data['name'] == df['buildingType'].iloc[i]]
    building_details_fk.append(int(building_details_data['id'].loc[(building_details_data['elevator']==elevator) & (building_details_data['building_structure_fk']==b_structure.iloc[0]) & (building_details_data['building_type_fk']==b_type.iloc[0])]))

    district_name = df['district'].iloc[i]
    district_fk.append(int(district_data['id'].loc[district_data['name']==district_name]))

sales_data = pd.DataFrame({'id':list(range(1,len(apartment_details_fk)+1)),'sale_date':df['sale_date'], 'sale_price':df['totalPrice'], 'avg_square_price':df['avg_square_price'], 'DOM':df['DOM'], 'year_built':df['constructionTime'], 'apartment_details_fk':apartment_details_fk, 'building_details_fk':building_details_fk, 'district_fk':district_fk})
sales_data.to_sql(con=mydb, name='sale', if_exists='append', index=False)



#DIMENSION   MODEL

#DIM_DATE
# get data from transaction model
metadata = db.MetaData()
sales = db.Table('sale', metadata, autoload=True, autoload_with=mydb)
query = db.select([sales.columns.sale_date])
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()

df_db = pd.DataFrame(ResultSet)
df_db.columns = ResultSet[0].keys()

# get data from CSV
CSV2_FILE_PATH = r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned_2.csv'
whole_file = pd.read_csv(CSV2_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1')
sale_column = whole_file['sale_date']
df_csv = sale_column.to_frame()

# merge into onde df
frames = [df_db, df_csv]
df_concat = pd.concat(frames) 
df_concat= df_concat.drop_duplicates()
df_concat = df_concat.reset_index(drop=True) 

# transform
date_pd = pd.DataFrame ({'date': df_concat['sale_date'],
						 'day': df_concat['sale_date'].apply(lambda x: x.day),
						 'month': df_concat['sale_date'].apply(lambda x: x.month),
						 'year': df_concat['sale_date'].apply(lambda x: x.year)})

# put to dimension model
dim_date_data = pd.DataFrame({'date_tk':list(range(1,len(date_pd)+1)),'date':date_pd['date'], 'day':date_pd['day'], 'month':date_pd['month'], 'year':date_pd['year']})
dim_date_data.to_sql(con=mydb, name='dim_date', if_exists='append', index=False)


#FACT_SALES - apartment_details_tk
CSV3_FILE_PATH = r'C:\Users\Luce\Projects\BeijingHousingPrice\CSV\BeijingHousingPrice_cleaned.csv'
df_file = pd.read_csv(CSV3_FILE_PATH, delimiter=',', parse_dates=["sale_date"], encoding='ISO-8859–1')
df_file = df_file.sort_values('sale_date')
df_file = df_file.reset_index(drop=True) 

metadata = db.MetaData()
dim_ad = db.Table('dim_apartment_details', metadata, autoload=True, autoload_with=mydb)
query = db.select([dim_ad])
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()

df_ad = pd.DataFrame(ResultSet)
df_ad.columns = ResultSet[0].keys()

apartment_details_tk = []
for i, row in df_file.iterrows(): 
    bedroom = df_file['bedroom'].iloc[i]
    kitchen = df_file['kitchen'].iloc[i]
    bathroom = df_file['bathRoom'].iloc[i]
    squares = df_file['square'].iloc[i]    
    renovation_cond = df_file['renovationCondition'].iloc[i]
    five_years_property = df_file['fiveYearsProperty'].iloc[i]
    tk = (df_ad['apartment_details_tk'].loc[(df_ad['bedroom']==bedroom) & (df_ad['kitchen']==kitchen) & (df_ad['bathroom']==bathroom) & (df_ad['squares']==squares) & (df_ad['renovation_condition']==renovation_cond) & (df_ad['five_years_property']==five_years_property)]).iloc[0]
    apartment_details_tk.append(tk)

df_tk = pd.DataFrame({'id':list(range(1,len(apartment_details_tk)+1)), 'apartment_details_tk': apartment_details_tk})
df_tk.to_sql(con=mydb, name='apartment_details_tk', if_exists='replace', index=False)
