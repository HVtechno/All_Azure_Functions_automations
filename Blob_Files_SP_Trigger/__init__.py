import datetime
import logging
import azure.functions as func
import pandas as pd
import os
import pyodbc
#DATETIME LIBRARIES
from datetime import date
#BLOB LIBRARIES
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
#SHAREPOINT LIBRARIES
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
#warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=UserWarning)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # GET application Variables from AZF

    blob_conn = os.environ['AzureWebJobsStorage']
    blob_container = os.environ['blob_container']
    DB_DRIVER = os.environ['DB_DRIVER']
    DB_SERVER = os.environ['DB_SERVER']
    DB_DATABASE = os.environ['DB_DATABASE']
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_TABLE_INV = os.environ['DB_TABLE_INV']
    DB_TABLE_FOL = os.environ['DB_TABLE_FOL']
    SP_SITE = os.environ['SP_SITE']
    SP_FOLDER = os.environ['SP_FOLDER']
    SP_USERNAME = os.environ['SP_USERNAME']
    SP_PASSWORD = os.environ['SP_PASSWORD']

    # Set up the connection to the blob storage
    # Connection string to BLOB storage
    MY_CONNECTION_STRING = blob_conn
    # Replace with blob container name
    MY_BLOB_CONTAINER = blob_container
    blob_service_client = BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(MY_BLOB_CONTAINER)
    #Connection to QA DB
    Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
    #logging.info("DB_connection_string" , Hconn)
    fileSOconn = pyodbc.connect(Hconn)

    logging.info('Get the SO for Blob files Connnected!!!')
    
    """
    Automation process
    STEP 1 : Loop through all the blobs, Get the list of blobs available on the container
    STEP 2 : If the Blobs available with .PDF extension proceed the steps
    STEP 3 : Run the Sql query to get the folderpaths for the files
    STEP 4 : Make a Dataframe
    STEP 5 : get the unique folder paths
    STEP 6 : If you have the folder path, fetch now the respective pdf file from blob and upload it to sharepoint
    STEP 7 : If no folder paths available, abort the process
    """

    try:
        for blob in container_client.list_blobs():
            if blob.name.lower().endswith('.pdf'):
                HVfile = blob.name
                HVfileName = blob.name.lower().replace('.pdf', '')
                logging.info(HVfileName)
                # ###Reading BLOB as binary
                blob_client = container_client.get_blob_client(HVfile)
                Bytes_Data = blob_client.download_blob().readall()
                ctx = ClientContext(SP_SITE).with_credentials(UserCredential(SP_USERNAME, SP_PASSWORD))
                list_title = SP_FOLDER
                ###Upload file to the testing folder with todays date
                HVpath = list_title + "/" + "TEST/INV_PLTesting/Prod" + date.today().strftime("%d%m%Y") + "/"
                target_folder = ctx.web.ensure_folder_path(HVpath).execute_query()
                name = os.path.basename(blob.name)
                target_file = target_folder.upload_file(name, Bytes_Data).execute_query()           
                logging.info("File has been uploaded to url: {0}".format(target_file.serverRelativeUrl))
                
                fileSO = pd.read_sql_query('''
                       SELECT H.file_name,V.folder_path
                        FROM ''' + DB_TABLE_INV + ''' as H
                        LEFT JOIN ''' + DB_TABLE_FOL + ''' as V ON 
                        IIF(LEFT(H.Sales_Document,2)='00',RIGHT(H.Sales_Document,8),H.Sales_Document) = V.sales_order_number
                        WHERE H.file_name LIKE ''' + "'" + HVfileName + "%'" ,fileSOconn)
                HVfileshare = pd.DataFrame(fileSO)
                HVfilekey = HVfileshare['folder_path'].unique()
                logging.info(HVfilekey)
                


                #if (HVfilekey != ''):
                if len(HVfilekey) > 0:
                #if (HVfilekey != None):
                    for i in HVfilekey:
                        ##### UPLOADING DIRECTLY ON THE SHAREPOINT FOLDER ####
                        HVpath = list_title + "/" + str(i)
                        logging.info(HVpath)
                        target_folder = ctx.web.ensure_folder_path(HVpath).execute_query()
                        name = os.path.basename(blob.name)
                        target_file = target_folder.upload_file(name, Bytes_Data).execute_query()
                else:
                    logging.error("For this File: " + str(HVfile) + " " + "Folder path not available in DB")
                DEL_container_client = ContainerClient.from_connection_string(conn_str=MY_CONNECTION_STRING, container_name=MY_BLOB_CONTAINER)
                DEL_container_client.delete_blob(blob=HVfile)
                logging.info("File: " + str(HVfile) + " " + "removed successfully from BLOB storage!!")     
               
    except Exception as e:
        logging.error("Error has occurred : " + str(e))

    logging.info('Python timer trigger function ran at %s', utc_timestamp)