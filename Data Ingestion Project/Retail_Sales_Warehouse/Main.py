#Import Required Libraries 
import sys
from sqlalchemy import text
from loguru import logger
 
# setting file path and logger path
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\execute_SQL_Command.py')
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\ETL_to_Psql_DW.py')
logger.add("Data Ingestion Project\Retail_Sales_Warehouse\logger.log")
from Components.execute_SQL_Command import execute_SQLQuery_method
from Components.ETL_to_Psql_DW import *

#Configuration Settings
logger.debug("Configuration Setting Initiated")
with open('config.json') as config_file:
    config_data = json.load(config_file)
    postsql_con_config = {"DB_NAME" : config_data["postsql_con_SITDB_Hank"]["DB_NAME"],
                        "DB_USER" : config_data["postsql_con_SITDB_Hank"]["DB_USER"],
                        "DB_PASS" : config_data["postsql_con_SITDB_Hank"]["DB_PASS"],
                        "DB_HOST" : config_data["postsql_con_SITDB_Hank"]["DB_HOST"],
                        "DB_PORT" : config_data["postsql_con_SITDB_Hank"]["DB_PORT"] }
    url = config_data["Mockaroo_API"]["Sales_Schema1"]

logger.debug("Configuration Setting Completed")

#Deploy Data Warehouse Models
logger.debug("Deploy Data Warehouse Model Initiated")
with open("Data Ingestion Project\Retail_Sales_Warehouse\Data Warehouse Model.sql") as file:
        query = file.read()
        execute_SQLQuery_method(query , postsql_con_config)
        file.close()

logger.debug("Deploy Data Warehouse Model Completed")
#Starting ETL Process
logger.debug("ETL Process Initiated")
logger.debug("Starting ETL Process .....")
extract_json_data = Extract_API_JSON(url)
df_extract = stage_data_into_pandas(extract_json_data)
print(df_extract)
logger.debug("ETL Process Completed")









