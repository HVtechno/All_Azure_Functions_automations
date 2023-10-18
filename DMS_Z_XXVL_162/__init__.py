import logging
import azure.functions as func
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
from io import BytesIO
import pyodbc
from fast_to_sql import fast_to_sql as fts
import time
import datetime
from datetime import timedelta
import openpyxl
import os
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    # GET ALL THE AZURE FUNCTIONS APPLICATION VARIABLES

    DB_DRIVER = os.environ['DB_DRIVER']
    DB_SERVER = os.environ['DB_SERVER']
    DB_DATABASE = os.environ['DB_DATABASE']
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    site_url = os.environ['DMS_Z_XXVL_162_SITE_URL']
    username = os.environ['SP_USERNAME']
    password = os.environ['SP_PASSWORD']
    folder_url = os.environ['DMS_Z_XXVL_162_SITE_FOLDER']
    DB_TABLE_1 = os.environ['DMS_Z_XXVL_162_DB_TABLE']
    DB_TABLE_2 = os.environ['DMS_Z_XXVL_162_LOGS_DB_TABLE']

    try:
        # AUTHENTICATE SHAREPOINT USING CREDENTIALS
        ctx_auth = AuthenticationContext(site_url)
        ctx_auth.acquire_token_for_user(username, password)
        ctx = ClientContext(site_url, ctx_auth)

        # GET ALL FILES FROM SHAREPOINT FOLDER
        folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        # GET ONLY FILES FROM LAST 3 WEEKS TILL DATE
        today = datetime.datetime.today()
        one_week_ago = today - timedelta(weeks=1)

        #DELETE DATA FROM TABLE
        Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
        logging.info(Hconn)
        cnxn = pyodbc.connect(Hconn)
        cnxn1 = pyodbc.connect(Hconn)

        #CHECK IF CONNECTION EXIST
        with cnxn.cursor() as cursor:
            try:
                cnxn.execute("SELECT 1")
                logging.info("Database Connected successfully!!!")
                # LOOP THROUGH EACH EXCEL FILE THAT HAS BEEN UPLOADED FROM LAST 3 WEEKS
                for file in files:
                    file_name = file.properties["Name"]
                    if file_name.endswith(".xlsx"):
                        file_name_without_extension = file_name[:-6]
                        file_date = datetime.datetime.strptime(file.properties['TimeLastModified'], '%Y-%m-%dT%H:%M:%SZ')
                        if file_date >= one_week_ago and file_date <= today:
                            logging.info(file.properties['Name'])
                            # OPEN THE BINARY FILE FROM SHAREPOINT & SEND THE RESPONSE SO THAT FILE SAVED ON THE MEMORY
                            response = File.open_binary(ctx, file.serverRelativeUrl)
                            if response.status_code == 200 and response.headers.get("Content-Type") == "application/octet-stream":
                                file_content = response.content
                                # READ THE FILE CONTENT FROM THE RESPONSE RECEIVED
                                Sharepointdata = pd.read_excel(BytesIO(file_content))
                                HV_sharedata = pd.DataFrame(Sharepointdata)
                                HV_sharedata['SalesDocno.'] = HV_sharedata['SalesDocno.'].astype(str)
                                HV_sharedata['Qty Delivered'] = HV_sharedata['Qty Delivered'].astype(float)
                                HV_sharedata['Material'] = HV_sharedata['Material'].astype(str).str.rstrip('.0')
                                HV_sharedata['S-PONo'] = HV_sharedata['S-PONo'].astype(str).str.rstrip('.0')
                                HV_sharedata['Sold-to'] = HV_sharedata['Sold-to'].astype(str)
                                HV_sharedata['Del Blk'] = HV_sharedata['Del Blk'].astype(str)
                                HV_sharedata['Bill-to'] = HV_sharedata['Bill-to'].astype(str)
                                HV_sharedata['Ship-to'] = HV_sharedata['Ship-to'].astype(str)
                                HV_sharedata['Invoice'] = HV_sharedata['Invoice'].astype(str).str.rstrip('.0')
                                HV_sharedata['Cred Blk'] = HV_sharedata['Cred Blk'].astype(str)
                                HV_sharedata['Invoice Currency'] = HV_sharedata['Invoice Currency'].astype(str)
                                HV_sharedata['Number of Pallets'] = HV_sharedata['Number of Pallets'].astype(int)
                                HV_sharedata['Doc Curr'] = HV_sharedata['Doc Curr'].astype(str)
                                HV_sharedata['Port of loading'] = HV_sharedata['Port of loading'].astype(str)
                                HV_sharedata['Port of Destination'] = HV_sharedata['Port of Destination'].astype(str)
                                HV_sharedata['S-PO-GR-TotQty'] = HV_sharedata['S-PO-GR-TotQty'].astype(float)
                                HV_sharedata['S-PO-IR-TotQty'] = HV_sharedata['S-PO-IR-TotQty'].astype(float)
                                HV_sharedata['Gross Wt From Del.'] = HV_sharedata['Gross Wt From Del.'].astype(float)
                            
                                HV_sharedata['PO Date'] = pd.to_datetime(HV_sharedata['PO Date'],errors='coerce')
                                HV_sharedata['PO Date'] = HV_sharedata['PO Date'].dt.strftime('%Y-%m-%d')
                            
                                HV_sharedata['Pr Date in SO'] = pd.to_datetime(HV_sharedata['Pr Date in SO'],errors='coerce')
                                HV_sharedata['Pr Date in SO'] = HV_sharedata['Pr Date in SO'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Billing Date'] = pd.to_datetime(HV_sharedata['Billing Date'],errors='coerce')
                                HV_sharedata['Billing Date'] = HV_sharedata['Billing Date'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Invoice Creation Date'] = pd.to_datetime(HV_sharedata['Invoice Creation Date'],errors='coerce')
                                HV_sharedata['Invoice Creation Date'] = HV_sharedata['Invoice Creation Date'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Invoice Value'] = HV_sharedata['Invoice Value'].astype(float)

                                HV_sharedata['Ord Cr Date'] = pd.to_datetime(HV_sharedata['Ord Cr Date'],errors='coerce')
                                HV_sharedata['Ord Cr Date'] = HV_sharedata['Ord Cr Date'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Cargo Cut off date'] = pd.to_datetime(HV_sharedata['Cargo Cut off date'],errors='coerce')
                                HV_sharedata['Cargo Cut off date'] = HV_sharedata['Cargo Cut off date'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Del Date in SO'] = pd.to_datetime(HV_sharedata['Del Date in SO'],errors='coerce')
                                HV_sharedata['Del Date in SO'] = HV_sharedata['Del Date in SO'].dt.strftime('%Y-%m-%d')

                                HV_sharedata['Gross Value/Order'] = HV_sharedata['Gross Value/Order'].astype(float)
                                DMS_columns = ['SalesDocno.','Qty Delivered','Material','S-PONo','Sold-to','Del Blk','Bill-to','Ship-to',
                                        'Invoice','Cred Blk','Invoice Currency','Number of Pallets','Doc Curr','Port of loading',
                                        'Port of Destination','S-PO-GR-TotQty','S-PO-IR-TotQty','Gross Wt From Del.','PO Date',
                                        'Pr Date in SO','Billing Date','Invoice Creation Date','Invoice Value','Ord Cr Date','Cargo Cut off date','Del Date in SO','Gross Value/Order']
                                Final_DF_DF = HV_sharedata[DMS_columns]    
                                Final_DF_DF.columns = ['SalesDocno','Qty Delivered','Material','S-PONo','Sold-to','Del Blk','Bill-to',
                                        'Ship-to','Invoice','Cred Blk','Invoice Currency','Number of Pallets','Doc Curr',
                                        'Port of loading','Port of Destination','S-PO-GR-TotQty','S-PO-IR-TotQty','Gross Wt From Del',
                                        'PO Date','Pr Date in SO','Billing Date','Invoice Creation Date','Invoice Value','Ord Cr Date',
                                        'Cargo Cut off date','Del Date in SO','Gross Value/Order']
                                mapping = {Final_DF_DF.columns[0]:'SalesDocno',Final_DF_DF.columns[1]:'Qty Delivered',Final_DF_DF.columns[2]:'Material',Final_DF_DF.columns[3]:'S-PONo',
                                    Final_DF_DF.columns[4]:'Sold-to',Final_DF_DF.columns[5]:'Del Blk',Final_DF_DF.columns[6]:'Bill-to',Final_DF_DF.columns[7]:'Ship-to',
                                    Final_DF_DF.columns[8]:'Invoice',Final_DF_DF.columns[9]:'Cred Blk',Final_DF_DF.columns[10]:'Invoice Currency',Final_DF_DF.columns[11]:'Number of Pallets',
                                    Final_DF_DF.columns[12]:'Doc Curr',Final_DF_DF.columns[13]:'Port of loading',Final_DF_DF.columns[14]:'Port of Destination',Final_DF_DF.columns[15]:'S-PO-GR-TotQty',
                                    Final_DF_DF.columns[16]:'S-PO-IR-TotQty',Final_DF_DF.columns[17]:'Gross Wt From Del',Final_DF_DF.columns[18]:'PO Date',Final_DF_DF.columns[19]:'Pr Date in SO',
                                    Final_DF_DF.columns[20]:'Billing Date',Final_DF_DF.columns[21]:'Invoice Creation Date',Final_DF_DF.columns[22]:'Invoice Value',Final_DF_DF.columns[23]:'Ord Cr Date',
                                    Final_DF_DF.columns[24]:'Cargo Cut off date',Final_DF_DF.columns[25]:'Del Date in SO',Final_DF_DF.columns[26]:'Gross Value/Order'}
                                Final_DMS_DF = Final_DF_DF.rename(columns=mapping)
                                Final_DMS_DF.replace('nan' , '', inplace=True)
                                #DELETE DATA ROWS FROM DB
                                DEL_share = f"Delete FROM [dbo].[DMS_Z_XXVL_162] WHERE [SalesDocno] like '{file_name_without_extension}%'"
                                cursor.execute(DEL_share)
                                logging.info(file_name + ' ' + 'Records deleted successfully from DB Table!!!')
                                #IF DATABASE CONNECTED ,UPLOAD DATA TO TABLE & SAVE DATA IN LOGS TABLE
                                try:
                                    create_statement = fts.fast_to_sql(Final_DMS_DF, DB_TABLE_1, cnxn, if_exists="append", temp=False)
                                    cnxn.commit()
                                    new_row = {'File_Name': file_name,'Status': 'Data Loaded to DB Successfully','Timestamp': today}
                                    with cnxn1.cursor() as cursor1:
                                        try:
                                            cnxn1.execute("SELECT 1")
                                            logging.info("Logs Database Connected successfully!!!")
                                            cursor1.execute("INSERT INTO [dbo].[DMS_Z_XXVL_162_Logs] (File_Name, Status,Timestamp) VALUES (?, ?, ?)", new_row['File_Name'], new_row['Status'],new_row['Timestamp'])
                                            cnxn1.commit()
                                        except:
                                            logging.info('Logs Database Not connected')
                                except pyodbc.Error:
                                #IF CONNECTION ERROR OCCURRED, DONT UPLOAD ANY FILES & SAVE DATA IN LOGS TABLE
                                    new_row = {'File_Name': file_name,'Status': 'Data not Loaded to DB due to connection issues','Timestamp':today}
                                    with cnxn1.cursor() as cursor1:
                                        try:
                                            cnxn1.execute("SELECT 1")
                                            logging.info("Logs Database Connected successfully!!!")
                                            cursor1.execute("INSERT INTO [dbo].[DMS_Z_XXVL_162_Logs] (File_Name, Status,Timestamp) VALUES (?, ?, ?)", new_row['File_Name'], new_row['Status'],new_row['Timestamp'])
                                            cnxn1.commit()
                                        except:
                                            logging.info('Logs Database Not connected')
                                finally:
                                    time.sleep(0.2*60)
                            # IF RESPONSE DIDNT RECEIVED,             
                            else:
                                logging.info(f"Error downloading file {file_name}")
                                logging.info(f"Status code: {response.status_code}")
                                logging.info(f"Content type: {response.headers.get('Content-Type')}")
            #IF DB CONNECTION NOT ESTABLISHED, CATCH ERROR
            except (pyodbc.Error, pyodbc.Warning) as e:
                logging.info(e)
                logging.info("Sorry, Database is not Connected and process has been aborted!!!")
    #IF SHAREPOINT AUTHENTICATION NOT SUCCEED, CATCH ERROR
    except Exception as ex:
        logging.info("Error: {0}".format(ex))                               

    logging.info('Python timer trigger function ran at %s', utc_timestamp)