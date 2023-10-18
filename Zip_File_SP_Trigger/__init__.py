import datetime
import logging
import azure.functions as func
#Dotenv Libraries
#from dotenv import load_dotenv
import os
#Pandas Libraries
import pandas as pd
#Sql libraries
import pyodbc
#Zip libraries
import zipfile
#Memory libraries
from io import BytesIO
#Office365 libraries
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
#Warnings libraries
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    DB_DRIVER = os.environ['DB_DRIVER']
    DB_SERVER = os.environ['DB_SERVER']
    DB_DATABASE = os.environ['DB_DATABASE']
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_TABLE_TC = os.environ['DB_TABLE_TC']
    DB_TABLE_MSC = os.environ['DB_TABLE_MSC']
    SP_SITE = os.environ['SP_SITE']
    SP_FOLDER = os.environ['SP_FOLDER']
    SP_USERNAME = os.environ['SP_USERNAME']
    SP_PASSWORD = os.environ['SP_PASSWORD']

    Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
    logging.info(Hconn)
    cnxn = pyodbc.connect(Hconn)

    try:
        cnxn.execute("SELECT 1")
        logging.info("Database is connected")

        ZIP_pd = pd.read_sql_query('''
            SELECT 
                TC.sales_order_number,
                TC.booking_number,
                TC.customer_purchase_order_number,
                MDSC.shipping,
                TC.folder_path 
                FROM dbo.DMS_Ticket_Creation AS TC
                LEFT JOIN MD_Shipping_Condition AS MDSC ON MDSC.shipping_condition = TC.shipping_conditions
                WHERE ticket_upload_status = 'Ready'

                UNION

                SELECT 
                SE.sales_order_number,
                SE.booking_number,
                SE.customer_purchase_order_number,
                MDSC.shipping,
                SE.folder_path 
                FROM dbo.DMS_Send_Email AS SE
                LEFT JOIN MD_Shipping_Condition AS MDSC ON MDSC.shipping_condition = SE.shipping_conditions
                WHERE send_email_status = 'Ready'
        ''',cnxn)
        ZIP_HV = pd.DataFrame(ZIP_pd)
        ZIP = ZIP_HV['folder_path'].unique()
        logging.info(ZIP)
        if (ZIP.size > 0):
            for i in ZIP:
            #CONNECTION TO SHAREPOINT
                ctx = ClientContext(SP_SITE).with_credentials(UserCredential(SP_USERNAME, SP_PASSWORD))    
                list_title = SP_FOLDER
                ##### ENSURE FOLDER IS AVAILABLE ON THE SHAREPOINT ####
                HVpath = list_title + "/" + i
                target_folder = ctx.web.ensure_folder_path(HVpath).execute_query()
                logging.info(HVpath + " " + "is available")
                files_ZIP = target_folder.files.get().execute_query()
                #CHECK IF PDF FILE & ZIP FILE AVAILABLE
                PDF_files = [f for f in files_ZIP if f.properties["Name"].lower().endswith(".pdf")]
                zip_files = [f for f in files_ZIP if f.properties["Name"].lower().endswith(".zip")]
                #IF PDF FILE AVAILABLE
                if len(PDF_files) > 0:
                    if len(zip_files) > 0:
                        logging.info("There is already one file with the .zip extension exist on this folder : " + HVpath)
                    else:
                        # CREATE ZIP FILE IN MEMORY
                        zip_data = BytesIO()
                        with zipfile.ZipFile(zip_data, mode='w') as zip_file:
                        # ADD EACH PDF FILE TO THE ZIPFILE
                            for file in PDF_files:
                                file_name = file.properties["Name"]
                                file_content = file.read()
                                zip_file.writestr(file_name, file_content)
                            zip_file.close()
                        zip_data.seek(0)
                        ## DELIMIT TARGET FOLDER WITH "/"
                        string_value = i
                        separated_values = string_value.split("/")
                        if "Truck" in separated_values:
                            PO_number = separated_values.index("Truck") + 1
                            if PO_number < len(separated_values):
                                PO_data = separated_values[PO_number]
                                ZIP_file_name = PO_data + ".zip"
                                target_file = target_folder.upload_file(ZIP_file_name, zip_data.getvalue()).execute_query()
                                logging.info("ZIPFile uploaded to url: {0}".format(target_file.serverRelativeUrl))
                        if "Ocean" in separated_values:
                            Book_number = separated_values.index("Ocean") + 1
                            if Book_number < len(separated_values):
                                Book_data = separated_values[Book_number]
                                ZIP_file_name = Book_data + ".zip"
                                target_file = target_folder.upload_file(ZIP_file_name, zip_data.getvalue()).execute_query()
                                logging.info("ZIPFile has been uploaded to url: {0}".format(target_file.serverRelativeUrl))
                else:
                    logging.info("There are no files with the .pdf extension in the folder.")
        else:
            logging.info("Upload Status is Ready However Folder_path is empty. So aborted the process")

    except pyodbc.Error:
        logging.info("Database is not connected. please check the connection")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)