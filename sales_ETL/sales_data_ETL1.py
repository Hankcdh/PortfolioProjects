
import pandas as pd 

df = pd.read_csv(r"C:\Users\Hank\OneDrive\Documents\PortfolioProjects\Datasets\supermarket_sales.csv")
df_test = df.head()
print(df_test)


import psycopg2
conn = psycopg2.connect(host="localhost", dbname="sales_database", user="postgres",  password = "hank69874" )
conn.set_session(autocommit= True)
cur = conn.cursor()
cur.execute("DELETE FROM sales")
print(df_test.columns)

# Transforming " Invoice ID" from String to Integers 
df_test['Invoice ID'] = df_test['Invoice ID'].apply(lambda row : row.replace("-" , ""))
print(df_test)
df_test['Invoice ID'] = df_test['Invoice ID'].astype(int)
print(df_test.dtypes)

df_target = df_test[['Invoice ID', 'Branch', 'City', 'Customer type' , 'Gender' , 'Total']]
print(df_target)
sql = "INSERT INTO sales (invoice,branch,city,customer_type,gender,total)  VALUES (%s,%s,%s,%s,%s,%s)"
for i,row in df_target.iterrows():
        cur.execute(sql , tuple(row))

sql2 = "select* from public.sales"
cur.execute(sql2)
# Fetch all the records
result = cur.fetchall()
for i in result:
    print(i)

conn.close()


