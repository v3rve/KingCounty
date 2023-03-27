#import libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import logging

#set script parameters
host_name = "mel.db.elephantsql.com"
db_name="uonxjsqy"
user_name="uonxjsqy"
pwd="onRoes84nrhjJULfwtat7_eSklSk7irr"
tab_name_main="kingcounty.main_table"
file_path_main="https://github.com/v3rve/KingCounty/raw/main/kc_house_data.csv"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

#Define connection function
def connect_db(_host_name, _db_name, _user_name, _pwd):
    try:
        conn = psycopg2.connect(
            host=_host_name,
            database=_db_name,
            user=_user_name,
            password=_pwd,
            port = '5432'
        )
        logging.info("Connected to database")
    except Exception as e:
        logging.error(f"Unable to connect to database: {e}")
    return(conn)

#Execute function to connect to DB
conn = connect_db(_host_name=host_name, _db_name=db_name, _user_name=user_name, _pwd=pwd)

# Define the function to create the table in the PostgreSQL database
def create_table(connection_db, table_name):
    try:
        conn = connection_db
        logging.info("Connected to database - tab creation")
    except Exception as e:
        logging.error(f"Unable to connect to database: {e}")
        return

    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS """ + table_name + """(
                id BIGINT,
                date DATE,
                price VARCHAR,
                bedrooms VARCHAR,
                bathrooms VARCHAR,
                sqft_living INTEGER,
                sqft_lot INTEGER,
                floors VARCHAR,
                waterfront INTEGER,
                view INTEGER,
                condition INTEGER,
                grade INTEGER,
                sqft_above INTEGER,
                sqft_basement INTEGER,
                yr_built INTEGER,
                yr_renovated INTEGER,
                zipcode INTEGER,
                lat VARCHAR,
                long VARCHAR,
                sqft_living15 INTEGER,
                sqft_lot15 INTEGER
            );
        """)
        conn.commit()
        logging.info("Table created successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating table: {e}")
        cursor.close()
        conn.close()
        return

    cursor.close()
    #conn.close()

# Define the function to read data from a CSV file and insert it into a PostgreSQL table
def df_to_postgres(connection_db, _file_path, table_name):

    # Read the CSV file into a pandas DataFrame
    try:
        df = pd.read_csv(_file_path)
        logging.info("CSV file read into DataFrame")
    except Exception as e:
        logging.error(f"Unable to read CSV file: {e}")
        return

    # establish a connection to the PostgreSQL database
    try:
        conn = connection_db
        logging.info("Connected to database")
    except Exception as e:
        logging.error(f"Unable to connect to database: {e}")
        return

    # create a cursor object
    cursor = conn.cursor()

    # iterate over the rows of the DataFrame and insert each row into the PostgreSQL table
    try:
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO "+ table_name +" (id, date, price, bedrooms, bathrooms, sqft_living, sqft_lot, floors, waterfront, "
                "view, condition, grade, sqft_above, sqft_basement, yr_built, yr_renovated, zipcode, lat, long, sqft_living15, sqft_lot15) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(row)
            )
            conn.commit()
            logging.info("Row: " + str(index) + " data inserted.")
        logging.info("Data inserted successfully")
    except Exception as e:
        conn.rollback
        logging.error(f"Error inserting data: {e}")
        cursor.close()
        conn.close()
        return

    # close the cursor and connection
    cursor.close()
    conn.close()

#Execute function to create table in DB
create_table(connection_db = conn, table_name = tab_name_main)

# Define the DAG and its parameters
dag = DAG(
     'data_to_postgresql',
     description='Import data from CSV to PostgreSQL',
     schedule_interval=None,
     start_date=datetime(2023, 3, 26),
     catchup=False
)
# # Define the PythonOperator that will execute the df_to_postgres() function
import_data_task = PythonOperator(
     task_id='import_data_to_db_main',
     python_callable=df_to_postgres(connection_db=conn,
                                    _file_path=file_path_main,
                                    table_name = tab_name_main),
     dag=dag
)

# Set the task dependencies
import_data_task
