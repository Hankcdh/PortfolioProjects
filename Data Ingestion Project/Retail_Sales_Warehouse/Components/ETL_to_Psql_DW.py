
from sqlalchemy import create_engine , text
import requests
import json
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from loguru import logger
from sqlalchemy import insert

def Extract_API_JSON(url):
    """
    Retrieves data from a specified API endpoint using HTTP GET request.
    
    Args:
        url (str): The URL of the API endpoint to retrieve data from.

    Returns:
        str: A formatted JSON string representing the response data.

    Raises:
        requests.exceptions.HTTPError: If the HTTP response status is not in the 2xx range.
        requests.exceptions.ReadTimeout: If the request times out.
        requests.exceptions.ConnectionError: If a connection error occurs.
        requests.exceptions.RequestException: For other types of request exceptions.

    Overview:
        This function creates a session object for making HTTP requests, sets authentication
        credentials, and includes a custom header in the request. It then performs a GET request
        to the specified API endpoint, handles various exceptions that may occur during the request,
        and returns a formatted JSON string representing the response data. The session is closed
        after a successful execution, and specific error messages are printed for different types of
        exceptions.

    Example Usage:
        url = "https://api.example.com/data"
        response_data = Get_API_Data(url)
        print(response_data)
    """

    try:
        # Create a session object for making HTTP requests
        with requests.Session() as s:
            # Set username and password for authentication
            s.auth = ('user', 'pass')

            # Update headers to include a custom 'x-test' field
            s.headers.update({'x-test': 'true'})

            # Make a GET request to the specified URL
            response = s.get(url)

            # Raise an HTTPError for bad responses (non-2xx status codes)
            response.raise_for_status()

            # Return the JSON response
            response_dict = response.json()
            return json.dumps(response_dict, indent=4, sort_keys=True)

        print("Session is closed")

    except requests.exceptions.HTTPError as errh: 
        # Handle HTTP errors (e.g., 4xx or 5xx status codes)
        print("HTTP Error") 
        print(errh.args[0]) 
    except requests.exceptions.ReadTimeout as errrt: 
        # Handle timeout errors during the request
        print("Time out") 
    except requests.exceptions.ConnectionError as conerr: 
        # Handle connection errors (e.g., network issues)
        print("Connection error") 
    except requests.exceptions.RequestException as errex: 
        # Handle other types of request exceptions
        print(f"Request Exception: {errex}")



def stage_data_into_pandas(json_data):
    df_staged = pd.read_json(json_data)
    return df_staged

def Transform_data_into_DW_Schema(df):
    df_prodDim = df[['SKU_Number' , 'Product_Description' , 'Brand_Description']]
    df_cashierDim = df[['Cashier_Employee_ID' , 'Cashier_Name']]
    df_retailsaleFact = df.drop(columns = ['Product_Description',  'Brand_Description','Cashier_Employee_ID' ])
    return df_prodDim , df_retailsaleFact , df_cashierDim

def ingest_data_into_productDim(df_data , postsql_con_config):
    conn_string = f'postgresql://{postsql_con_config["DB_USER"]}:{postsql_con_config["DB_PASS"]}@{postsql_con_config["DB_HOST"]}:{postsql_con_config["DB_PORT"]}/{postsql_con_config["DB_NAME"]}'

    try:
        # Connection Establishment
        SQLAlchemy_engine = create_engine(conn_string)
        session = Session(SQLAlchemy_engine)
        print("Database connected successfully")

        # SQL Query Execution
        # for index, row in df_data.iterrows():
        #     #sql_query = f"INSERT INTO productdim (sku_number,product_description,brand_description) VALUES ({row['SKU_Number']}, {row['Product_Description']},{row['Brand_Description']})"
        #     #session.execute(text("INSERT INTO productdim (sku_number,product_description,brand_description) values(?,?,?)", row.SKU_Number, row.Product_Description, row.Brand_Description))

        
        # session.commit()
        session.new
        print(session.new)
        print("PostgreSQL Command successfully")

    except SQLAlchemyError as err:
        # Specific SQLAlchemyError for better exception handling
        print(f"SQLAlchemyError: {err}")
        session.rollback()

    except Exception as err:
        # Generic exception block for unexpected errors
        print(f"Unexpected Error: {err}")
        session.rollback()

    finally:
        # Connection Cleanup
        session.close()


   

def Load_data_into_DW_Schema(df_prodDim, df_cashierDim , df_retailsaleFact,Conn_Config):
    #Loading Dimension Table
    try:
        ingest_data_into_productDim(df_prodDim, Conn_Config)
    except Exception as err:
        print(err)
    


#Test Case 
with open('config.json') as config_file:
    config_data = json.load(config_file)
    postsql_con_config = {"DB_NAME" : config_data["postsql_con_SITDB_Hank"]["DB_NAME"],
                        "DB_USER" : config_data["postsql_con_SITDB_Hank"]["DB_USER"],
                        "DB_PASS" : config_data["postsql_con_SITDB_Hank"]["DB_PASS"],
                        "DB_HOST" : config_data["postsql_con_SITDB_Hank"]["DB_HOST"],
                        "DB_PORT" : config_data["postsql_con_SITDB_Hank"]["DB_PORT"] }
    url = config_data["Mockaroo_API"]["Sales_Schema1"]

json_data = Extract_API_JSON(url)
df_staged = stage_data_into_pandas(json_data)
logger.debug("Data Staged Completed")
df_prodDim , df_retailsaleFact , df_cashierDim = Transform_data_into_DW_Schema(df_staged)
logger.debug("Data Transformation Completed")
#print(df_prodDim , df_retailsaleFact , df_cashierDim)
Load_data_into_DW_Schema(df_prodDim , df_retailsaleFact , df_cashierDim,postsql_con_config)