import datetime
import logging
import os
# DATAFRAME LIBRARIES
import pandas as pd
#SQL READ & UPLOAD LIBRARIES
import pyodbc
from fast_to_sql import fast_to_sql as fts
#BLOB LIBRARIES
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
#SHAREPOINT LIBRARIES
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
#AZURE FUNCTION
import azure.functions as func

# GET ALL THE APPLICATION VARIABLES
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
Folder_Path_Query = os.environ['FOLDER_BLOB_QUERY']
SplitBookingsQuery = os.environ['DMS_Folder_Paths_SplitBookings']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
site_url = os.environ['FOLDER_SITE_URL']
username = os.environ['SP_USERNAME']
password = os.environ['SP_PASSWORD']

# CONNECTION STRING FOR SQL SERVER
Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
cnxn = pyodbc.connect(Hconn)
cnxn1 = pyodbc.connect(Hconn)
cnxn_status = pyodbc.connect(Hconn)
cnxn_error = pyodbc.connect(Hconn)

# connect to blob
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
# Get the container client
container_client = blob_service_client.get_container_client(container_name)
# Get the blob client for the query file
blob_client = container_client.get_blob_client(Folder_Path_Query)
# Download the query file as text
Folder_query_text = blob_client.download_blob().readall().decode("utf-8")
#Get the Split bookings query
blob_client = container_client.get_blob_client(SplitBookingsQuery)
Split_bookings_query_text = blob_client.download_blob().readall().decode("utf-8")


#Declare cursor
cursor_error = None
cursor_upload = None
cursor_success = None
cursor_status = None

def log_error_and_abort(message):
    # DATETIME
    today = datetime.datetime.today()
    try:
        logging.error(message)
        cursor_error = cnxn_error.cursor()
        new_row = {'Status': message, 'Total_Rows_Uploaded': '','Timestamp': today}
        cursor_error.execute("INSERT INTO [dbo].[DMS_Folder_Paths_Logs] (Status, Total_Rows_Uploaded, Timestamp) VALUES (?, ?, ?)",
                            new_row['Status'], new_row['Total_Rows_Uploaded'],new_row['Timestamp'])
        cnxn_error.commit()
        # Abort the process
    except SystemExit as se:
        # Handle the SystemExit exception here
        logging.warning("SystemExit exception caught: {}".format(se))
        return "Function completed successfully"
    finally:
        if cursor_error:
            cursor_error.close()
    
def split_bookings_handler():
    cursor_split = cnxn.cursor()
    cursor_split.execute("SELECT 1")
    logging.info("Database connected successfully")
    cursor_split.execute(Split_bookings_query_text)
    logging.info("Split Bookings Query successfully Executed. Split bookings were updated")
    cursor_split.close()

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # Create a BlobServiceClient to access the storage account
    if not blob_service_client:
        log_error_and_abort("Failed to connect to Blob storage. So, Aborted the process")
    
    ################################## NOT NEEDED ##############################################
    # Create a sharepoint access
    #ctx_auth = AuthenticationContext(url=site_url)
    #if ctx_auth.acquire_token_for_user(username=username, password=password):
    #    ctx = ClientContext(site_url, ctx_auth)
    #    logging.info("Sharepoint Authentication Success!!")
    #else:
    #    log_error_and_abort("Failed to connect to sharepoint. So, Aborted the process")
    ################################### NOT NEEDED #############################################

    try:
        split_bookings_handler()
    except Exception as e:
        logging.info(e)

    try:
        #Define all the cursor with connection here
        cursor_upload = cnxn.cursor()
        cursor_success = cnxn1.cursor()
        cursor_status = cnxn_status.cursor()
        # DATETIME
        today = datetime.datetime.today()
        logging.info("Database connected successfully")
        DMS_meta_ocean_truck = pd.read_sql_query(Folder_query_text, cnxn)
        if DMS_meta_ocean_truck.empty:
            log_error_and_abort("No folder paths returned from the query. So, Aborted the process")
        else:
            Final_DF = pd.DataFrame(DMS_meta_ocean_truck)
            Final_DF['shipto_country'] = Final_DF['shipto_country'].replace(r'[~\!\@\#\$\%\^\*\"\'\?\,\.\|\_\{\}\[\]\/\–\-\:\;]', '', regex=True)
            Final_DF['shipto_name'] = Final_DF['shipto_name'].replace(r'[~\!\@\#\$\%\^\*\"\'\?\,\.\|\_\{\}\[\]\/\–\-\:\;]', '', regex=True)
            Final_DF['OTdata'] = Final_DF['OTdata'].replace(r'[~\!\@\#\$\%\^\*\"\'\?\,\.\|\_\{\}\[\]\/\–\-\:\;]', '', regex=True)
            Final_DF['folder_path'] = Final_DF['region'] + "/Shipments/" + Final_DF['shipto_country'] + "/" + Final_DF['shipto_number'] +  "-" + Final_DF['shipto_name'] + "/" + Final_DF['shipp_condition'] + "/" +  Final_DF['OTdata']
            Final_DF = Final_DF.loc[Final_DF['folder_path'].notnull()]
            del Final_DF['OTdata']
            DMS_columns = ['sales_order_number','booking_number','po_number','shipp_condition','shipto_number','shipto_name','shipto_country','region','folder_path','status']
            Final_DF_DF = Final_DF[DMS_columns]    
            Final_DF_DF.columns = ['sales_order_number','booking_number','po_number','shipp_condition','shipto_number','shipto_name','shipto_country','region','folder_path','status']
            mapping = {
                Final_DF_DF.columns[0]:'sales_order_number',
                Final_DF_DF.columns[1]:'booking_number',
                Final_DF_DF.columns[2]:'po_number',
                Final_DF_DF.columns[3]:'shipp_condition',
                Final_DF_DF.columns[4]:'shipto_number',
                Final_DF_DF.columns[5]:'shipto_name',
                Final_DF_DF.columns[6]:'shipto_country',
                Final_DF_DF.columns[7]:'region',
                Final_DF_DF.columns[8]:'folder_path',
                Final_DF_DF.columns[9]:'status'
                }
            Final_DMS_DF = Final_DF_DF.rename(columns=mapping)
            #deleting all data rows on the table
            logging.info('Total Rows: ' + str(len(Final_DMS_DF)))
            DEL_folder_path = ''' 
                Delete FROM [dbo].[DMS_Folder_Paths] WHERE status IS NULL
                '''
            cursor_upload.execute(DEL_folder_path)
            logging.info('Records where status is null has been Deleted Successfully!!') 
            create_statement = fts.fast_to_sql(Final_DMS_DF, "DMS_Folder_Paths", cnxn, if_exists="append", temp=False)
            cnxn.commit()
            logging.info('Folder paths Uploaded to sql successfully!!')
            
            #Update status with success status
            new_row = {'Status': 'Folder paths Uploaded to sql successfully' , 'Total_Rows_Uploaded':len(Final_DMS_DF),'Timestamp': today}
            cursor_success.execute("INSERT INTO [dbo].[DMS_Folder_Paths_Logs] (Status, Total_Rows_Uploaded, Timestamp) VALUES (?, ?, ?)",
                                    new_row['Status'], new_row['Total_Rows_Uploaded'], new_row['Timestamp'])
            cnxn1.commit()
            
            #delete the Blank values from the column folder_path
            #create an another column final_path by concatenating with Shared%20Documents(root folder) with SQL folder_path
            #replace some special characters (''')
            #get the unique values of the column
            #connect to sharepoint & then upload folder structure to sharepoint to create folders
            Final_DMS_DF.rename(columns = {'[sales_order_number]':'sales_order_number','[booking_number]':'booking_number','[po_number]':'po_number','[shipp_condition]':'shipp_condition','[shipto_number]':'shipto_number','[shipto_name]':'shipto_name','[shipto_country]':'shipto_country','[region]':'region','[folder_path]':'folder_path','[status]':'status'},inplace = True)
            Final_DMS_DF['Final_path'] = "Shared%20Documents" + "/" + Final_DMS_DF['folder_path'].astype(str)
            Species_folder_upload = Final_DMS_DF['Final_path'].unique()
            
            batch_size = 100
            num_folders = len(Species_folder_upload)

            for i in range(0, num_folders, batch_size):
                batch_folders = Species_folder_upload[i:i+batch_size]
                # Create a new SharePoint connection for each batch
                ctx_auth = AuthenticationContext(url=site_url)
                if ctx_auth.acquire_token_for_user(username=username, password=password):
                    ctx = ClientContext(site_url, ctx_auth)
                    try:
                        for folder_path in batch_folders:
                            logging.info(folder_path.replace('Shared%20Documents/', '') + ' ' + "Authentication success to create folder")
                            target_folder = ctx.web.ensure_folder_path(folder_path).execute_query()
                            cursor_status.execute("UPDATE [dbo].[DMS_Folder_Paths] SET status = 'Done' WHERE [folder_path]='"+folder_path.replace('Shared%20Documents/', '')+"'")
                            cnxn_status.commit()
                    except Exception as e:
                        logging.error(f"Failed to create folders in the batch: {str(e)}")
                else:
                    log_error_and_abort("Failed to connect to SharePoint. So, Aborted the process")

###########################################NOT NEEDED, WILL TRY BATCH PROCESSING ######################################################                        
            #for folder_path in Species_folder_upload:
                # Create a sharepoint access for each folderpath
            #    ctx_auth = AuthenticationContext(url=site_url)
            #    if ctx_auth.acquire_token_for_user(username=username, password=password):
            #        ctx = ClientContext(site_url, ctx_auth)
            #        try:
            #            logging.info(folder_path.replace('Shared%20Documents/','') + ' ' + "Authentication success to create folder")
            #            target_folder = ctx.web.ensure_folder_path(folder_path).execute_query()
            #            cursor_status.execute("UPDATE [dbo].[DMS_Folder_Paths] SET status = 'Done' WHERE [folder_path]='"+folder_path.replace('Shared%20Documents/','')+"'")
            #            cnxn_status.commit()
            #        except Exception as e:
            #            logging.error(f"Failed to create folder {folder_path}: {str(e)}")
            #    else:
            #        log_error_and_abort("Failed to connect to sharepoint. So, Aborted the process")
###########################################NOT NEEDED, WILL TRY BATCH PROCESSING ######################################################                        

    except Exception as e:
        # Handle the exception
        logging.error(f"An error occurred: {str(e)}")

    finally:
        if cursor_upload:
            cursor_upload.close()
        if cursor_success:
            cursor_success.close()
        if cursor_status:
            cursor_status.close()
            
    logging.info('Python timer trigger function ran at %s', utc_timestamp)