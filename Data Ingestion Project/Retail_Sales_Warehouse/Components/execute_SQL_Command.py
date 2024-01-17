from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError


def execute_SQLQuery_method(sql_query, postsql_con_config):
    """
    Executes a SQL query on a PostgreSQL database using the provided configuration.

    Parameters:
    - sql_query (str): The SQL query to be executed.
    - postsql_con_config (dict): Dictionary containing PostgreSQL connection details.
                                Example: {"DB_USER": "user", "DB_PASS": "password", "DB_HOST": "localhost", "DB_PORT": 5432, "DB_NAME": "database_name"}

    Returns:
    None

    Comments:
    1. This function connects to a PostgreSQL database using the provided connection configuration.
    2. It then executes the given SQL query using SQLAlchemy and commits the changes to the database.
    3. If any exception occurs during the connection or query execution, an error message is printed,
       and the transaction is rolled back to maintain data integrity.

    Example Usage:
    ```python
    sql_query = "SELECT * FROM users;"
    connection_config = {"DB_USER": "user", "DB_PASS": "password", "DB_HOST": "localhost", "DB_PORT": 5432, "DB_NAME": "database_name"}
    execute_SQLQuery_method(sql_query, connection_config)
    ```

    Note:
    - Ensure that the required SQLAlchemy and PostgreSQL libraries are installed before using this function.
    - Proper exception handling is implemented to print error messages and rollback transactions in case of failure.
    """
    # Parameter Validation
    if not isinstance(sql_query, str) or not sql_query.strip():
        raise ValueError("The SQL query must be a non-empty string.")

    required_keys = ["DB_USER", "DB_PASS", "DB_HOST", "DB_PORT", "DB_NAME"]
    if not all(key in postsql_con_config for key in required_keys):
        raise ValueError("Incomplete PostgreSQL connection configuration. Required keys: {}".format(required_keys))

    conn_string = f'postgresql://{postsql_con_config["DB_USER"]}:{postsql_con_config["DB_PASS"]}@{postsql_con_config["DB_HOST"]}:{postsql_con_config["DB_PORT"]}/{postsql_con_config["DB_NAME"]}'

    try:
        # Connection Establishment
        SQLAlchemy_engine = create_engine(conn_string)
        session = Session(SQLAlchemy_engine)
        print("Database connected successfully")

        # SQL Query Execution
        session.execute(text(sql_query))
        session.commit()
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

