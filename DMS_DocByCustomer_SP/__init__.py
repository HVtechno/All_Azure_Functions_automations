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
                    Sharepointdata = pd.read_excel(BytesIO(file_content), sheet_name='Documents')
                    HV_sharedata = pd.DataFrame(Sharepointdata)
                    # CONDITIONAL FILTERING (HARD CODED IN SHIPTO TO EXCLUDE '29744324')
                    if (HV_sharedata['Shipto'].isin([29744324]).any()):
                        HV_sharedata = HV_sharedata.loc[HV_sharedata['Shipto'] != 29744324]
                    if (HV_sharedata['Export Zone'].str.contains(str("Not found")).any()):
                        HV_sharedata = HV_sharedata.loc[HV_sharedata['Export Zone'].str.contains(str("Not found")) == False]
                    # if (HV_sharedata['Status'].str.contains(str("Blocked")).any()):
                    #     HV_sharedata = HV_sharedata.loc[HV_sharedata['Status'].str.contains(str("Blocked")) == False]
                    
                    # RENAME COLUMNS AS PER DATABASE COLUMN AND READY TO UPLOAD
                    HV_sharedata.columns = ['Export Zone', 'Country', 'Country Code', 'Sales Org', 'Soldto', 'Sold to name',
                                            'Shipto', 'Ship to name', 'Payer', 'Payer name', 'Status', 'Sh.Cond code', 'Shipping',
                                            'Prepayment', 'KPI', 'Customer type', 'Sending System', 'DOCS', 'BL', 'INVOICE', 'PL',
                                            'COO', 'COA/CAT', 'COH', 'COI', 'Other documents', 'Special Requirements', 'Annual docs',
                                            'Hard copy docs', 'Documentation Contact', 'DHL address', 'Tips & tricks',
                                            'Documentation owner', 'Back up 1', 'Back up 2', 'note']
                    
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
                        HV_sharedata.columns[11]: 'shipping_conditions',
                        HV_sharedata.columns[12]: 'shipping',
                        HV_sharedata.columns[13]: 'prepayment',
                        HV_sharedata.columns[14]: 'kpi',
                        HV_sharedata.columns[15]: 'customer_type',
                        HV_sharedata.columns[16]: 'sending_system',
                        HV_sharedata.columns[17]: 'docs',
                        HV_sharedata.columns[18]: 'bill_of_lading',
                        HV_sharedata.columns[19]: 'invoice',
                        HV_sharedata.columns[20]: 'packing_list',
                        HV_sharedata.columns[21]: 'coo',
                        HV_sharedata.columns[22]: 'coa_cat',
                        HV_sharedata.columns[23]: 'coh',
                        HV_sharedata.columns[24]: 'coi',
                        HV_sharedata.columns[25]: 'other_documents',
                        HV_sharedata.columns[26]: 'special_requirements',
                        HV_sharedata.columns[27]: 'annual_docs',
                        HV_sharedata.columns[28]: 'hard_copy_docs',
                        HV_sharedata.columns[29]: 'documentation_contact',
                        HV_sharedata.columns[30]: 'dhl_address',
                        HV_sharedata.columns[31]: 'tips_tricks',
                        HV_sharedata.columns[32]: 'documentation_owner',
                        HV_sharedata.columns[33]: 'back_up1',
                        HV_sharedata.columns[34]: 'back_up2',
                        HV_sharedata.columns[35]: 'note'
                    }
                    
                    Final_HV_sharedata = HV_sharedata.rename(columns=mapping)
                    
                    # Adjust data types for specific columns
                    DOCS_data_types = {
                            'docs': 'VARCHAR(MAX)',
                            'other_documents': 'VARCHAR(MAX)',
                            'special_requirements': 'VARCHAR(MAX)', 
                            'annual_docs': 'VARCHAR(MAX)',
                            'hard_copy_docs': 'VARCHAR(MAX)',
                            'documentation_contact': 'VARCHAR(MAX)',
                            'dhl_address': 'VARCHAR(MAX)',
                            'tips_tricks': 'VARCHAR(MAX)',
                            'note': 'VARCHAR(MAX)'
                            }
                    
                    # UPLOAD DATAFRAME TO DATABASE
                    with cnxn.cursor() as cursor:
                        #DATETIME
                        today = datetime.datetime.today()
                        try:
                            ## DROP TABLE ##
                            logging.info("Database Connected successfully!!!")
                            DEL_share = '''DROP TABLE IF EXISTS [dbo].[MD_Doc]'''
                            cursor.execute(DEL_share)
                            logging.info('Docs By Customer Table has been dropped successfully!!')
                            
                            ## CREATE TABLE ##
                            table_name = 'MD_Doc'
                            table_columns = ', '.join(f'{col} {DOCS_data_types.get(col, "VARCHAR(255)")}' for col in Final_HV_sharedata.columns)
                            create_table_sql = f'CREATE TABLE {table_name} ({table_columns})'
                            cursor.execute(create_table_sql)
                            logging.info('Docs By Customer Table has been successfully created!!')
                            
                            ## IMPORT DATA ##
                            create_statement = fts.fast_to_sql(Final_HV_sharedata, "MD_Doc", cnxn, if_exists="append", temp=False)
                            new_row = {'Status': 'DOCS By Customer Loaded to DB Successfully', 'Total_Rows_Uploaded':len(Final_HV_sharedata),'Timestamp': today}
                            logging.info('DOCS By Customer data Loaded to DB Successfully!!') 
                            cnxn.commit()
                        except (pyodbc.Error, pyodbc.Warning) as e:
                            cnxn.rollback()
                            new_row = {'Status': 'Check Issue description' + ' ' + e, 'Total_Rows_Uploaded':'','Timestamp': today}
                            
                        with cnxn1.cursor() as cursor1:
                            cursor1.execute("INSERT INTO [dbo].[MD_Doc_Logs] (Status,Total_Rows_Uploaded,Timestamp) VALUES (?, ?, ?)", new_row['Status'],new_row['Total_Rows_Uploaded'],new_row['Timestamp'])
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