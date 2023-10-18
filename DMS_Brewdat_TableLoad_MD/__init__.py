import pyodbc
import logging
import datetime
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings, ContainerClient
import os

#GET ALL ENVIRONMENT VARIABLES
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
SQLconn_str = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
databricks_server_hostname = os.environ['DMS_DATABRICKS_HOST']
databricks_http_path = os.environ['DMS_DATABRICKS_PATH']
databricks_access_token = os.environ['DMS_DATABRICKS_TOKEN']

#Batch_size
batch_size = 50000

# Logging setup
logging.basicConfig(level=logging.INFO)

#Query_Mapping (removed MD_Un_locode as its not required for daily refresh 
#because in databricks its refreshing once in an year)

query_mapping = {
    'SELECT_MD_PLANT.sql': {
        'insert_query_file': 'INSERT_MD_PLANT.sql',
        'table_name': 'MD_Plant'
    },
    'SELECT_ERP_CUSTOMERLIST.sql': {
        'insert_query_file': 'INSERT_ERP_CUSTOMERLIST.sql',
        'table_name': 'MD_Customer_List'
    },
    'SELECT_MD_SKU.sql': {
        'insert_query_file': 'INSERT_MD_SKU.sql',
        'table_name': 'MD_SKU'
    },
}

def connect_to_sql():
    try:
        cnxn = pyodbc.connect(SQLconn_str)
        return cnxn
    except pyodbc.Error as e:
        logging.error("Error connecting to SQL Server: %s", str(e))
        raise

def connect_to_databricks():
    try:
        from databricks import sql as databricks_sql
        conn = databricks_sql.connect(
            server_hostname=databricks_server_hostname,
            http_path=databricks_http_path,
            access_token=databricks_access_token
        )
        return conn
    except Exception as e:
        logging.error("Error connecting to Databricks: %s", str(e))
        raise

def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()
    except Exception as e:
        logging.error("Error executing delete query: %s", str(e))
        raise

def delete_data_from_table(conn, table_name):
    try:
        query = f"DELETE FROM {table_name}"
        execute_query(conn, query)
    except Exception as e:
        logging.error("Error deleting data from table: %s", str(e))
        raise

def fetch_data_from_databricks(conn, query, batch_size):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = []
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            rows.extend(batch)
        cursor.close()
        return rows
    except Exception as e:
        logging.error("Error fetching data from Databricks: %s", str(e))
        raise

def insert_data_into_table(conn, query, data, batch_size):
    try:
        cursor = conn.cursor()
        start = 0
        end = batch_size
        while start < len(data):
            batch = data[start:end]
            cursor.fast_executemany = True
            try:
                cursor.executemany(query, batch)
                conn.commit()
            except pyodbc.DataError as e:
                logging.warning("Data error occurred during insert: %s", str(e))
                for row in batch:
                    try:
                        cursor.execute(query, row)
                        conn.commit()
                    except pyodbc.DataError as e:
                        logging.error("Error inserting row: %s", str(e))
                        # Handle the error as desired (e.g., skip the row or log the error)
            start = end
            end += batch_size
        cursor.close()
    except Exception as e:
        logging.error("Error inserting data into table: %s", str(e))
        raise

def download_query_file_from_blob(storage_conn_str, container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        query_text = blob_client.download_blob().readall().decode("utf-8")
        return query_text
    except Exception as e:
        logging.error("Error downloading query file from blob storage: %s", str(e))
        raise

def log_script_status(conn, table_name, status):
    try:
        timestamp = datetime.datetime.today()
        with conn.cursor() as cursor:
            load_data = pd.read_sql_query('''SELECT * FROM ''' + table_name,conn)
            logging.info(load_data)
            load_data_df = pd.DataFrame(load_data)
            query = "INSERT INTO [dbo].[DMS_Brewdat_Logs] (Table_Name, Status, Total_rows_Uploaded, Timestamp) VALUES (?, ?, ?, ?)"
            cursor.execute(query, table_name, status, len(load_data_df), timestamp)
            conn.commit()
    except Exception as e:
        logging.error("Error logging script status: %s", str(e))
        raise

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
    tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    try:
        SQLconn = connect_to_sql()
        logging.info("Database connected successfully!")

        for select_query, query_info in query_mapping.items():
            # Download the query files from Azure Storage
            Select_query_text = download_query_file_from_blob(storage_connection_string, container_name, select_query)
            Insert_query_text = download_query_file_from_blob(storage_connection_string, container_name, query_info['insert_query_file'])

            # Connect to Databricks
            Brickconn = connect_to_databricks()
            logging.info("Databricks connection established successfully!")

            # Fetch data from Databricks
            rows = fetch_data_from_databricks(Brickconn, Select_query_text,batch_size)
            logging.info("Data fetched from Databricks")

            # Clear the existing data in MD_SKU table
            delete_data_from_table(SQLconn, query_info['table_name'])
            logging.info("Data deleted from %s table", query_info['table_name'])

            # Insert data into SQL Server
            insert_data_into_table(SQLconn, Insert_query_text, rows,batch_size)
            logging.info("Data inserted into SQL Server")

            # Log script status
            log_script_status(SQLconn, query_info['table_name'], "Data loaded successfully")

            # Close connections
            #Brickconn.close()
            #SQLconn.close()

    except Exception as e:
        logging.error("An error occurred during the script execution: %s", str(e))

    logging.info('Python timer trigger function ran at %s', utc_timestamp)