import pandas as pd 
df = pd.read_csv(r"C:\Users\Hank\OneDrive\Documents\PortfolioProjects\Datasets\supermarket_sales.csv")
df_test = df.head()
print(df_test)




import psycopg2 
import pandas as pd 
from sqlalchemy import create_engine 
  
  
conn_string = 'postgresql://postgres:hank69874@localhost/sales_database'
  
db = create_engine(conn_string) 
conn = db.connect() 

# Transforming " Invoice ID" from String to Integers 
df_test['Invoice ID'] = df_test['Invoice ID'].apply(lambda row : row.replace("-" , ""))
print(df_test)
df_test['Invoice ID'] = df_test['Invoice ID'].astype(int)
print(df_test.dtypes)

# our dataframe 
data = df_test


# Create DataFrame 
df = pd.DataFrame(data) 
df.to_sql('data', con=conn, if_exists='replace', 
          index=False) 
conn = psycopg2.connect(conn_string) 
conn.autocommit = True
cursor = conn.cursor() 


sql1 = '''select * from data;'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i) 
  
# conn.commit() 
conn.close() 