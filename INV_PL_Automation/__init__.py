import logging
import pyodbc
from fast_to_sql import fast_to_sql as fts
import json
import os
import azure.functions as func
import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

# GET ENV Application variable
CORS_URLS = os.environ['CORS_URLs'].split(',')
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
INV_PL_Query = os.environ['DMS_INV_BLOB_QUERY']

# CONNECTION STRING FOR SQL SERVER
Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
cnxn = pyodbc.connect(Hconn)
cnxn1 = pyodbc.connect(Hconn)
cnxn_df = pyodbc.connect(Hconn)
#Define all the cursor with connection here
cursor_upload = cnxn.cursor()
    
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
# Get the container client
container_client = blob_service_client.get_container_client(container_name)
# Get the blob client for the query file
blob_client = container_client.get_blob_client(INV_PL_Query)
# Download the query file as text
INV_PL_Query_text = blob_client.download_blob().readall().decode("utf-8")

def log_error_and_abort(message):
    #Datetime
    today = datetime.datetime.today()
    try:
        logging.error(message)
        new_row = {'Status': message, 'Total_Rows_Available': '','Timestamp': today}
        with cnxn1.cursor() as cursor_error:
            cursor_error.execute("INSERT INTO [dbo].[INV_PL_automation_Logs] (Status, Total_Rows_Available, Timestamp) VALUES (?, ?, ?)",
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
    
    if not blob_service_client:
        log_error_and_abort("Failed to connect to Blob storage. So, Aborted the process")
    else:
        DMS_INV_PL = pd.read_sql_query(INV_PL_Query_text,cnxn)
        DMS_INV_PL_df = pd.DataFrame(DMS_INV_PL)
        if DMS_INV_PL_df.empty:
            log_error_and_abort("No INV/PL returned from the query. So, Aborted the process")
        else:
            today = datetime.datetime.today()
            DEL_INV_PL_path = ''' 
                Delete FROM [dbo].[INV_PL_Automation]
                '''
            cursor_upload.execute(DEL_INV_PL_path)
            logging.info('Records has been Deleted Successfully!!') 
            create_statement = fts.fast_to_sql(DMS_INV_PL_df, "INV_PL_Automation", cnxn, if_exists="append", temp=False)
            cnxn.commit()

            new_row = {'Status': 'Data Loaded successfully', 'Total_Rows_Available': len(DMS_INV_PL_df),'Timestamp': today}
            with cnxn_df.cursor() as cursor_insert:
                cursor_insert.execute("INSERT INTO [dbo].[INV_PL_automation_Logs] (Status, Total_Rows_Available, Timestamp) VALUES (?, ?, ?)",
                      new_row['Status'], new_row['Total_Rows_Available'],new_row['Timestamp'])
                cnxn_df.commit()
            cursor_insert.close()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
