
# import required modules
import requests
import json



# url = 'https://fakestoreapi.com/products'
test_url = "https://my.api.mockaroo.com/testschema.json?key=444e9ff0" 

def print_json_response(response):
    """
    Print the JSON response with indentation and sorted keys.

    Args:
        response (requests.Response): The HTTP response object containing JSON data.
    """
    response_dict = response.json()
    print(json.dumps(response_dict, indent=4, sort_keys=True))

def Get_API_Data(url):
    """
    Retrieve data from a specified API endpoint using HTTP GET request.

    Args:
        url (str): The URL of the API endpoint to retrieve data from.

    Raises:
        requests.exceptions.HTTPError: If the HTTP response status is not in the 2xx range.
        requests.exceptions.ReadTimeout: If the request times out.
        requests.exceptions.ConnectionError: If a connection error occurs.
        requests.exceptions.RequestException: For other types of request exceptions.

    Overview:
        This function creates a session object for making HTTP requests, sets authentication
        credentials, and includes a custom header in the request. It then performs a GET request
        to the specified API endpoint, handles various exceptions that may occur during the request,
        and prints the JSON response with the help of the print_json_response function. The session
        is closed after a successful execution, and specific error messages are printed for different
        types of exceptions.

    Example Usage:
        url = "https://api.example.com/data"
        Get_API_Data(url)
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

            # Print the JSON response
            print_json_response(response)

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


Get_API_Data(test_url)