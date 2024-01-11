from sqlalchemy import create_engine
from sqlalchemy import URL
from sqlalchemy import text
from sqlalchemy.orm import Session


def execute_SQLQuery_method(sql_query , postsql_con_config):
    
    conn_string = f'postgresql://{postsql_con_config["DB_USER"]}:{postsql_con_config["DB_PASS"]}@{postsql_con_config["DB_HOST"]}:{postsql_con_config["DB_PORT"]}/{postsql_con_config["DB_NAME"]}'

    try :
        SQLAlchemy_engine = create_engine(conn_string)
        session = Session(SQLAlchemy_engine)
        print("Database connected successfully")
    except Exception as err :
         print(err)
         print("Database connect Unsuccessfully")

    try:

        # if (isinstance(type(sql_query), type(text.__class__)) == False):
        #     raise  TypeError("The SQL_query must be in sqlalchemy text type")
        
        session.execute(sql_query)
        session.commit()
        print("Postgresql Command successfully")
    except Exception as err:
        # pass exception to function
        # print_psycopg2_exception(err)
        print(err)
        # rollback the previous transaction before starting another
        session.rollback()



# Main Test
postsql_con_config = {"DB_NAME" : "sit_db",
                     "DB_USER" : "Hank",
                     "DB_PASS" : "hank69874",
                     "DB_HOST" : "localhost",
                     "DB_PORT" : "5433" }
sql_query = text(" SELECT * FROM mock_users")

execute_SQLQuery_method(sql_query , postsql_con_config)