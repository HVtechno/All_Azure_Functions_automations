import datetime
import logging
import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts
import os
from io import BytesIO
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import azure.functions as func

# GET ALL THE AZURE FUNCTIONS APPLICATION VARIABLES
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
site_url = os.environ['DOCS_SITE_URL']
file_path = os.environ['DOCS_FILE_PATH']
username = os.environ['SP_USERNAME']
password = os.environ['SP_PASSWORD']

#SQL CONNECTION
Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
logging.info(Hconn)
cnxn = pyodbc.connect(Hconn)
cnxn1 = pyodbc.connect(Hconn)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    try:
        # AUTHENTICATE & CREATE OFFICE365 CONTEXT
        ctx_auth = AuthenticationContext(url=site_url)
        if ctx_auth.acquire_token_for_user(username=username, password=password):
            ctx = ClientContext(site_url, ctx_auth)
            logging.info("Authentication Success!!")
            
            file= ctx.web.get_file_by_server_relative_url(file_path)
            if file:
                ctx.load(file)
                ctx.execute_query()
                logging.info("File has been found, Will try to send the response so that content can be read!!")
                response = File.open_binary(ctx, file.serverRelativeUrl)
                if response.status_code == 200 and response.headers.get("Content-Type") == "application/octet-stream":
                    file_content = response.content
                    # READ EXCEL & CONVERT TO DATAFRAME (SHEET: 'Documents')
                    Sharepointdata = pd.read_excel(BytesIO(file_content), sheet_name='SI&BL')
                    HV_sharedata = pd.DataFrame(Sharepointdata)
                    # RENAME COLUMNS AS PER DATABASE COLUMN AND READY TO UPLOAD
                    HV_sharedata.columns = ['Export Zone', 'Country', 'Country Code', 'Sales Org', 'Soldto', 'Sold to name',
                                            'Shipto', 'Ship to name', 'Payer', 'Payer name', 'Status', 'Shipping','Incoterm',
                                            'Incoterm 2','Door/Port','CNEE','NTFY 1','NTFY 2','Consignee email','BL type (SWB/OBL)',
                                            'Other BL handling Instructions','Remarks in BL','HNumRNum','BL receiver','Freight type',
                                            'Ocean Payable','Origin haulage charges (precarriage)','OTHC Terminal Handling Cost at Origin (Origin port charges)',
                                            'Sea Freight','DTHC Terminal Handling Cost at Destination (Destination port charges)',
                                            'Destination haulage charges (oncarriage)','Combination','Merge',
                                            'SI template','Buyco ID','SI Comments','Sending type']
                    
                    # Add the new column "BAF" and set its values as the same as "sea_freight"
                    HV_sharedata.insert(29,'BAF', HV_sharedata['Sea Freight'])
                    
                    mapping = {
                        HV_sharedata.columns[0]: 'export_zone',
                        HV_sharedata.columns[1]: 'country',
                        HV_sharedata.columns[2]: 'country_code',
                        HV_sharedata.columns[3]: 'sales_organization',
                        HV_sharedata.columns[4]: 'sold_to_number',
                        HV_sharedata.columns[5]: 'sold_to_name',
                        HV_sharedata.columns[6]: 'ship_to_number',
                        HV_sharedata.columns[7]: 'ship_to_name',
                        HV_sharedata.columns[8]: 'payer_number',
                        HV_sharedata.columns[9]: 'payer_name',
                        HV_sharedata.columns[10]: 'status',
                        HV_sharedata.columns[11]: 'shipping',
                        HV_sharedata.columns[12]: 'incoterm',
                        HV_sharedata.columns[13]: 'incoterm_2',
                        HV_sharedata.columns[14]: 'door_port',
                        HV_sharedata.columns[15]: 'CNEE',
                        HV_sharedata.columns[16]: 'NTFY_1',
                        HV_sharedata.columns[17]: 'NTFY_2',
                        HV_sharedata.columns[18]: 'consignee_email',
                        HV_sharedata.columns[19]: 'BL_type',
                        HV_sharedata.columns[20]: 'BL_instructions',
                        HV_sharedata.columns[21]: 'remarks_BL',
                        HV_sharedata.columns[22]: 'HNumRNum',
                        HV_sharedata.columns[23]: 'BL_receiver',
                        HV_sharedata.columns[24]: 'freight_type',
                        HV_sharedata.columns[25]: 'ocean_payable',
                        HV_sharedata.columns[26]: 'precarriage',
                        HV_sharedata.columns[27]: 'origin_port_changes',
                        HV_sharedata.columns[28]: 'sea_freight',
                        HV_sharedata.columns[29]: 'BAF',
                        HV_sharedata.columns[30]: 'destination_port_changes',
                        HV_sharedata.columns[31]: 'on_carriage',
                        HV_sharedata.columns[32]: 'combination',
                        HV_sharedata.columns[33]: 'merge_freights',
                        HV_sharedata.columns[34]: 'SI_template',
                        HV_sharedata.columns[35]: 'buyco_ID',
                        HV_sharedata.columns[36]: 'SI_comments',
                        HV_sharedata.columns[37]: 'Sending_type'
                    }
                    
                    Final_HV_sharedata = HV_sharedata.rename(columns=mapping)
                    
                    # Adjust data types for specific columns
                    DOCS_data_types = {
                            'CNEE': 'VARCHAR(MAX)',
                            'NTFY_1': 'VARCHAR(MAX)',
                            'NTFY_2': 'VARCHAR(MAX)', 
                            'consignee_email': 'VARCHAR(MAX)',
                            'BL_instructions': 'VARCHAR(MAX)',
                            'remarks_BL': 'VARCHAR(MAX)',
                            'SI_comments': 'VARCHAR(MAX)'
                            }
                    
                    # Define the newline character to replace
                    newline_character = '\n'

                    # Iterate through all columns and replace the newline character
                    for column in Final_HV_sharedata.columns:
                        Final_HV_sharedata[column] = Final_HV_sharedata[column].str.replace(newline_character, ' ')

                    # UPLOAD DATAFRAME TO DATABASE
                    with cnxn.cursor() as cursor:
                        #DATETIME
                        today = datetime.datetime.today()
                        try:
                            ## DROP TABLE ##
                            logging.info("Database Connected successfully!!!")
                            DEL_share = '''DROP TABLE IF EXISTS [dbo].[MD_Doc_Cus_Freights]'''
                            cursor.execute(DEL_share)
                            logging.info('Docs By Customer Table has been dropped successfully!!')
                            
                            ## CREATE TABLE ##
                            table_name = 'MD_Doc_Cus_Freights'
                            table_columns = ', '.join(f'{col} {DOCS_data_types.get(col, "VARCHAR(255)")}' for col in Final_HV_sharedata.columns)
                            create_table_sql = f'CREATE TABLE {table_name} ({table_columns})'
                            cursor.execute(create_table_sql)
                            logging.info('Docs By Customer Table has been successfully created!!')
                            
                            ## IMPORT DATA ##
                            create_statement = fts.fast_to_sql(Final_HV_sharedata, "MD_Doc_Cus_Freights", cnxn, if_exists="append", temp=False)
                            new_row = {'Status': 'DOCS Freights data Loaded to DB Successfully', 'Total_Rows_Uploaded':len(Final_HV_sharedata),'Timestamp': today}
                            logging.info('DOCS Freights data Loaded to DB Successfully!!') 
                            cnxn.commit()
                        except (pyodbc.Error, pyodbc.Warning) as e:
                            cnxn.rollback()
                            new_row = {'Status': 'Check Issue description' + ' ' + e, 'Total_Rows_Uploaded':'','Timestamp': today}
                            
                        with cnxn1.cursor() as cursor1:
                            cursor1.execute("INSERT INTO [dbo].[MD_Doc_Cus_Freights_Logs] (Status,Total_Rows_Uploaded,Timestamp) VALUES (?, ?, ?)", new_row['Status'],new_row['Total_Rows_Uploaded'],new_row['Timestamp'])
                            logging.info("Status Updated to Logs successfully!!!")
                            cnxn1.commit()

                # IF RESPONSE DIDNT RECEIVED,             
                else:
                    logging.info(f"Error downloading file {file_path}")
                    logging.info(f"Status code: {response.status_code}")
                    logging.info(f"Content type: {response.headers.get('Content-Type')}")

                # ABORT THE PROCESS IF AUTHENTICATION FAILED
            else:
                logging.info("Error: Failed to get the file from SharePoint.")
        else:
            logging.info("Authentication failed!!")

    except Exception as error:
        logging.info(error) 

    logging.info('Python timer trigger function ran at %s', utc_timestamp)