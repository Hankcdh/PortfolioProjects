
import sys
from sqlalchemy import text
 
# setting path
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\execute_SQL_Command.py')
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\ETL_to_Psql_DW.py')

from Components.execute_SQL_Command import execute_SQLQuery_method
from Components.ETL_to_Psql_DW import *

#Configuration Settings
postsql_con_config = {"DB_NAME" : "sit_db",
                     "DB_USER" : "Hank",
                     "DB_PASS" : "hank69874",
                     "DB_HOST" : "localhost",
                     "DB_PORT" : "5433" }

API_URL = "https://my.api.mockaroo.com/sales_api___stage_1_.json?key=444e9ff0"
#sql_query = text(" SELECT * FROM mock_users")
with open("Data Ingestion Project\Retail_Sales_Warehouse\Data Warehouse Model.sql") as file:
        query = text(file.read())
        execute_SQLQuery_method(query , postsql_con_config)
        file.close()


#Starting ETL
print("Startin ETL")
print("Starting ")
extract_json_data = Extract_API_JSON(API_URL)
df_extract = stage_data_into_pandas(extract_json_data)
print(df_extract)








