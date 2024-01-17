
import sys
from sqlalchemy import text
 
# setting path
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\execute_SQL_Command.py')
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\ETL_to_Psql_DW.py')

from Components.execute_SQL_Command import execute_SQLQuery_method
from Components.ETL_to_Psql_DW import *

#Configuration Settings
with open('config.json') as config_file:
    config_data = json.load(config_file)
    postsql_con_config = {"DB_NAME" : config_data["postsql_con_SITDB_Hank"]["DB_NAME"],
                        "DB_USER" : config_data["postsql_con_SITDB_Hank"]["DB_USER"],
                        "DB_PASS" : config_data["postsql_con_SITDB_Hank"]["DB_PASS"],
                        "DB_HOST" : config_data["postsql_con_SITDB_Hank"]["DB_HOST"],
                        "DB_PORT" : config_data["postsql_con_SITDB_Hank"]["DB_PORT"] }
    url = config_data["Mockaroo_API"]["Sales_Schema1"]

#sql_query = text(" SELECT * FROM mock_users")
with open("Data Ingestion Project\Retail_Sales_Warehouse\Data Warehouse Model.sql") as file:
        query = file.read()
        execute_SQLQuery_method(query , postsql_con_config)
        file.close()


#Starting ETL
print("Startin ETL")
print("Starting .....")
extract_json_data = Extract_API_JSON(url)
df_extract = stage_data_into_pandas(extract_json_data)
print("ETL Completed")








