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
MD_AdditionalScript_Query = os.environ['MD_ADDITIONAL_SCRIPTS']
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
blob_client = container_client.get_blob_client(MD_AdditionalScript_Query)
# Download the query file as text
MD_AdditionalScript_Text = blob_client.download_blob().readall().decode("utf-8")

def log_error_and_abort(message):
    #Datetime
    try:
        logging.error(message)
    except SystemExit as se:
        logging.warning("SystemExit exception caught: {}".format(se))
        return "Function completed successfully"


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

        # Create a BlobServiceClient to access the storage account
    if not blob_service_client:
        log_error_and_abort("Failed to connect to Blob storage. So, Aborted the process")

    with cnxn.cursor() as upload_cursor:
        try:
            upload_cursor.execute("SELECT 1")
            logging.info("Database connected successfully")
            upload_cursor.execute(MD_AdditionalScript_Text)
            logging.info("Script was executed")
        except (pyodbc.Error, pyodbc.Warning) as e:
            #cnxn.rollback()
            logging.info(e)
        
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
