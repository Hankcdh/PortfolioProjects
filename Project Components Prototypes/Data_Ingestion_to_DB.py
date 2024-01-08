import requests
import json
import pandas as pd
import psycopg2 
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2 import __version__ as psycopg2_version
print ("psycopg2 version:", psycopg2_version, "\n")
import sys
from sqlalchemy import create_engine
from sqlalchemy import URL



def Get_API_Data(url):
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
    df = pd.read_json(json_data)
    return df

# define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")


def ingest_data_into_DB(df_data , postsql_con_config, table_name):
    conn_string = f'postgresql://{postsql_con_config["DB_USER"]}:{postsql_con_config["DB_PASS"]}@{postsql_con_config["DB_HOST"]}:{postsql_con_config["DB_PORT"]}/{postsql_con_config["DB_NAME"]}'

    try:
        # url_object = URL.create(
        #     "postgresql++psycopg2",
        #     username=postsql_con_config["DB_USER"],
        #     password=postsql_con_config["DB_PASS"],
        #     host=postsql_con_config["DB_HOST"],
        #     database=postsql_con_config["DB_NAME"],
        #     port=postsql_con_config["DB_PORT"],)
        SQLAlchemy_engine = create_engine(conn_string)

        conn = psycopg2.connect(database=postsql_con_config["DB_NAME"],
                                user=postsql_con_config["DB_USER"],
                                password=postsql_con_config["DB_PASS"],
                                host=postsql_con_config["DB_HOST"],
                                port=postsql_con_config["DB_PORT"])
        print("Database connected successfully")

    except OperationalError as err:
        # pass exception to function
        print_psycopg2_exception(err)
        print("Database not connected successfully")

    # if the connection was successful

    try:
        with SQLAlchemy_engine.connect() as SQLAlchemy_conn:
            df_data.to_sql(table_name, con=SQLAlchemy_conn, if_exists='append', index=False)

            SQLAlchemy_conn.commit()  # commit the transaction
        print("Postgresql Insert successfully")

    except Exception as err:
        # pass exception to function
        # print_psycopg2_exception(err)
        print(err)
        # rollback the previous transaction before starting another
        SQLAlchemy_conn.rollback()

        # try:
        #     cursor.execute(f"SELECT * FROM {table_name};")
        # except psycopg2.errors.FdwTableNotFound as err:
        #     # pass exception to function
        #     print_psycopg2_exception(err)

        # close the cursor object to avoid memory leaks

        # close the connection object also


#### Main Test 
print("Testing API calls..")
test_url = "https://my.api.mockaroo.com/testschema.json?key=444e9ff0" 
data = Get_API_Data(test_url)

print("Testing API Completed")
print("Testing stage_data_into_pandas..")
df = stage_data_into_pandas(data)
print(df)
print("stage_data_into_pandas Completed")

#Test Ingestion
postsql_con_config = {"DB_NAME" : "sit_db",
                     "DB_USER" : "test_user1",
                     "DB_PASS" : "test456",
                     "DB_HOST" : "localhost",
                     "DB_PORT" : "5433" }
table_name = "mock_users"
ingest_data_into_DB(df,postsql_con_config ,table_name)