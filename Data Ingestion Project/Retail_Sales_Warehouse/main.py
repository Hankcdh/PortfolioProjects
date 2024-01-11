
import sys
from sqlalchemy import text
 
# setting path
sys.path.append('Data Ingestion Project\Retail_Sales_Warehouse\Components\execute_SQL_Command.py')


from Components.execute_SQL_Command import execute_SQLQuery_method


postsql_con_config = {"DB_NAME" : "sit_db",
                     "DB_USER" : "Hank",
                     "DB_PASS" : "hank69874",
                     "DB_HOST" : "localhost",
                     "DB_PORT" : "5433" }
#sql_query = text(" SELECT * FROM mock_users")
with open("Data Ingestion Project\Retail_Sales_Warehouse\Data Warehouse Model.sql") as file:
        query = text(file.read())
        execute_SQLQuery_method(query , postsql_con_config)





