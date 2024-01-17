from Data_Ingestion_to_DB import Get_API_Data , stage_data_into_pandas , ingest_data_into_DB
import json




# Function Test "Data Ingestion TO DB"
with open('config.json') as config_file:
    config_data = json.load(config_file)
    postsql_con_config = {"DB_NAME" : config_data["postsql_con_SITDB_test"]["DB_NAME"],
                        "DB_USER" : config_data["postsql_con_SITDB_test"]["DB_USER"],
                        "DB_PASS" : config_data["postsql_con_SITDB_test"]["DB_PASS"],
                        "DB_HOST" : config_data["postsql_con_SITDB_test"]["DB_HOST"],
                        "DB_PORT" : config_data["postsql_con_SITDB_test"]["DB_PORT"] }
    test_url = config_data["Mockaroo_API"]["testURL"]
print("Testing API calls..")
data = Get_API_Data(test_url)

print("Testing API Completed")
print("Testing stage_data_into_pandas..")
df = stage_data_into_pandas(data)
print(df)
print("stage_data_into_pandas Completed")

#Test Ingestion

table_name = "mock_users"
ingest_data_into_DB(df,postsql_con_config ,table_name)