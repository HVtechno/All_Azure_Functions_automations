import datetime
import logging
import pyodbc
import pandas as pd
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

# GET ALL THE APPLICATION VARIABLES
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
KPI_Deadline = os.environ['DMS_KPI_DEADLINE_QUERY']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']

# CONNECTION STRING FOR SQL SERVER
Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
cnxn = pyodbc.connect(Hconn)
cnxn1 = pyodbc.connect(Hconn)
cnxn_df = pyodbc.connect(Hconn)
    
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
# Get the container client
container_client = blob_service_client.get_container_client(container_name)
# Get the blob client for the query file
blob_client = container_client.get_blob_client(KPI_Deadline)
# Download the query file as text
KPI_Deadline_text = blob_client.download_blob().readall().decode("utf-8")

def log_error_and_abort(message):
    #Datetime
    today = datetime.datetime.today()
    try:
        logging.error(message)
        new_row = {'Status': message, 'Total_Rows_Available': '','Timestamp': today}
        with cnxn1.cursor() as cursor_error:
            cursor_error.execute("INSERT INTO [dbo].[DMS_KPI_Deadline_Logs] (Status, Total_Rows_Available, Timestamp) VALUES (?, ?, ?)",
                            new_row['Status'], new_row['Total_Rows_Available'],new_row['Timestamp'])
            cnxn1.commit()
    except SystemExit as se:
        logging.warning("SystemExit exception caught: {}".format(se))
        return "Function completed successfully"
    finally:
        cursor_error.close()

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

        # Create a BlobServiceClient to access the storage account
    if not blob_service_client:
        log_error_and_abort("Failed to connect to Blob storage. So, Aborted the process")

    with cnxn.cursor() as upload_cursor:
        today = datetime.datetime.today()
        try:
            upload_cursor.execute("SELECT 1")
            logging.info("Database connected successfully")
            upload_cursor.execute(KPI_Deadline_text)
            logging.info("Ticket Query successfully Executed. Data loaded into Ticket query table")
            # DMS_Ticket_Query = pd.read_sql_query('''SELECT * FROM [dbo].[DMS_Ticket_Creation]''',cnxn_df)
            # DMS_Ticket_Query_df = pd.DataFrame(DMS_Ticket_Query)
            new_row = {'Status': 'Data Loaded successfully', 'Total_Rows_Available': 0,'Timestamp': today}
        except (pyodbc.Error, pyodbc.Warning) as e:
            #cnxn.rollback()
            logging.info(e)
            new_row = {'Status': 'SQL connection not established. So, Aborted the process', 'Total_Rows_Available': '','Timestamp': today}
        
        with cnxn1.cursor() as cursor_insert:
            cursor_insert.execute("INSERT INTO [dbo].[DMS_KPI_Deadline_Logs] (Status, Total_Rows_Available, Timestamp) VALUES (?, ?, ?)",
                      new_row['Status'], new_row['Total_Rows_Available'],new_row['Timestamp'])
            cnxn1.commit()
        cursor_insert.close()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
