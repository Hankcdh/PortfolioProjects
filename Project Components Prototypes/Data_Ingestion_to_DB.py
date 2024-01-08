import requests
import json
import pandas as pd



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



print("Testing API calls..")
test_url = "https://my.api.mockaroo.com/testschema.json?key=444e9ff0" 
data = Get_API_Data(test_url)

print(data)
print("Testing API Completed")
print("Testing stage_data_into_pandas..")
df = stage_data_into_pandas(data)
print(df)
print("stage_data_into_pandas Completed")

