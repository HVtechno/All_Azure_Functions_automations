import datetime
import logging
import azure.functions as func
import pyodbc
import os
from azure.storage.blob import BlobServiceClient, BlobClient

#GET APPLICATION VARIABLES SETTING
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
connection_string = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"

#QUERY FILES FROM BLOB
query_files = [
"DMS_CT_Destination.sql",
"DMS_CT_MD_Notify_Consignee.sql",
"DMS_CT_POL_POD.sql",
"DMS_CT_Shipper.sql",
"DMS_CT_shipto_Freight_DOCs.sql",
"DMS_CT_SKU_DESC.sql",
"DMS_CT_soldto.sql"
]

#BLOB CONNECTION
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)

def execute_query(connection_string, query, table_name):
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()

            # Update the log table with execution information
            Timestamp = datetime.datetime.now()
            Rows_inserted = cursor.rowcount  # Assuming rowcount gives you the number of rows affected/inserted
            log_query = f"INSERT INTO DMS_CT_Log (Table_name, Rows_data_available, Timestamp) VALUES (?, ?, ?)"
            cursor.execute(log_query, (table_name, Rows_inserted, Timestamp))
            conn.commit()

    except Exception as e:
        print(f"Error executing query: {e}")

def delete_table(connection_string, table_name):
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
    except Exception as e:
        print(f"Error deleting table: {e}")

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # Delete existing tables first
    delete_table(connection_string, "DMS_CT_Soldto")
    logging.info("DMS_CT_Soldto is deleted successfully")
    delete_table(connection_string, "DMS_CT_shipto_Freight_DOCs")
    logging.info("DMS_CT_shipto_Freight_DOCs is deleted successfully")
    delete_table(connection_string, "DMS_CT_Destination")
    logging.info("DMS_CT_Destination is deleted successfully")
    delete_table(connection_string, "DMS_CT_shipper")
    logging.info("DMS_CT_shipper is deleted successfully")
    delete_table(connection_string, "DMS_CT_POL_POD")
    logging.info("DMS_CT_POL_POD is deleted successfully")
    delete_table(connection_string, "DMS_CT_SKU_DESC")
    logging.info("DMS_CT_SKU_DESC is deleted successfully")
    delete_table(connection_string, "DMS_CT_MD_Notify_Consignee")
    logging.info("DMS_CT_MD_Notify_Consignee is deleted successfully") 

    for query_file in query_files:
        blob_name = query_file
        blob_client = container_client.get_blob_client(blob_name)
        query = blob_client.download_blob().readall().decode("utf-8")

        # Extract table name from query_file (assuming the query file names are consistent)
        table_name = query_file.replace(".sql", "")
        execute_query(connection_string, query, table_name)
        logging.info("Data is loaded now for {table_name}")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
