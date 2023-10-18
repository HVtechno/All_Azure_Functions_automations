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
import traceback
import pyxlsb

# GET ALL THE AZURE FUNCTIONS APPLICATION VARIABLES
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
site_url = os.environ['DMS_Proforma_SITE']
file_path = os.environ['DMS_Proforma_FILE_PATH']
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
                    # READ EXCEL & CONVERT TO DATAFRAME (SHEET: 'Proforma')
                    Sharepointdata = pd.read_excel(BytesIO(file_content), sheet_name='Proforma')
                    # Read from PGI excel
                    SharepointdataPGI = pd.read_excel(BytesIO(file_content), sheet_name='PGI')
                     # CHANGE FIELD 'Fecha facturacion' INTO DATETIME
                    Sharepointdata['Fecha facturacion'] = pd.to_datetime(Sharepointdata['Fecha facturacion'], unit='D', origin='1899-12-30')
                    HV_sharedata = pd.DataFrame(Sharepointdata)
                    HV_sharedataPGI = pd.DataFrame(SharepointdataPGI)
                    # UPDATE FIELD 'Marca' WITHOUT SUFFIX '.0'
                    HV_sharedata['Marca'] = HV_sharedata['Marca'].astype(str).apply(lambda x: x.replace('.0',''))
                    Req_col = ['Fecha facturacion','Centro','Folio de Referencia','Folio Fiscal',
                                            'Distribuidor','Marca','Descripción del material','Cartones','Importe Neto',
                                            'Caja','Sello transporte','Booking','kilos','Peso bruto']
                    HV_sharedata_req = HV_sharedata[Req_col]

                    Req_colPGI = ['Sales Order','MX SKU','ERP SKU']
                    HV_sharedata_reqPGI = HV_sharedataPGI[Req_colPGI]
                    # RENAME COLUMNS AS PER DATABASE COLUMN AND READY TO UPLOAD
                    
                    mapping = {
                        HV_sharedata_req.columns[0]: 'Fecha_facturacion',
                        HV_sharedata_req.columns[1]: 'Centro',
                        HV_sharedata_req.columns[2]: 'Folio_de_Referencia',
                        HV_sharedata_req.columns[3]: 'Folio_Fiscal',
                        HV_sharedata_req.columns[4]: 'Distribuidor',
                        HV_sharedata_req.columns[5]: 'Marca',
                        HV_sharedata_req.columns[6]: 'Descripcion_del_material',
                        HV_sharedata_req.columns[7]: 'Cartones',
                        HV_sharedata_req.columns[8]: 'Importe_Neto',
                        HV_sharedata_req.columns[9]: 'Caja',
                        HV_sharedata_req.columns[10]: 'Sello_transporte',
                        HV_sharedata_req.columns[11]: 'Booking',
                        HV_sharedata_req.columns[12]: 'kilos',
                        HV_sharedata_req.columns[13]: 'Peso_bruto'
                    }
                    
                    mappingPGI={
                    HV_sharedata_reqPGI.columns[0]: 'Sales_order_number',
                    HV_sharedata_reqPGI.columns[1]: 'MX_SKU',
                    HV_sharedata_reqPGI.columns[2]: 'ERP_SKU',
                    }
                    Final_HV_sharedata = HV_sharedata_req.rename(columns=mapping)
                    Final_HV_sharedataPGI = HV_sharedata_reqPGI.rename(columns=mappingPGI)

                    # ADJUST DATA TYPES FOR SPECIFIC COLUMNS
                    Proforma_data_types = {
                            'Fecha_facturacion': 'DATE',
                            'Distribuidor': 'VARCHAR(MAX)',
                            'Descripcion_del_material': 'VARCHAR(MAX)',
                            'Cartones': 'INT',
                            'Importe_Neto': 'DECIMAL(18,2)',
                            'kilos': 'DECIMAL(18,2)',
                            'Peso_bruto': 'DECIMAL(18,2)'
                            }
                    PGI_data_types_PGI = {
                        'Sales_order_number': 'VARCHAR(255)',
                        'MX_SKU': 'VARCHAR(255)',
                        'ERP_SKU':'VARCHAR(255)'
                            }

                    # UPLOAD DATAFRAME TO DATABASE
                    with cnxn.cursor() as cursor:
                        #DATETIME
                        today = datetime.datetime.today()
                        ## DROP TABLE ##
                        logging.info("Database Connected successfully!!!")
                        DEL_share = '''DROP TABLE IF EXISTS [dbo].[DMS_Proforma]'''
                        cursor.execute(DEL_share)
                        logging.info('Proforma Table has been dropped successfully!!')

                        DEL_sharePGI = '''DROP TABLE IF EXISTS [dbo].[DMS_PGI]'''
                        cursor.execute(DEL_sharePGI)
                        logging.info('PGI Table has been dropped successfully!!')
                        
                        ## CREATE TABLE ##
                        table_name = 'DMS_Proforma'
                        table_columns = ', '.join(f'{col} {Proforma_data_types.get(col, "VARCHAR(255)")}' for col in Final_HV_sharedata.columns)
                        create_table_sql = f'CREATE TABLE {table_name} ({table_columns})'
                        cursor.execute(create_table_sql)
                        logging.info('Proforma Table has been successfully created!!')

                        table_namePGI = 'DMS_PGI'
                        table_columnsPGI = ', '.join(f'{colPgi} {PGI_data_types_PGI.get(colPgi, "VARCHAR(255)")}' for colPgi in Final_HV_sharedataPGI.columns)
                        create_table_sql_PGI = f'CREATE TABLE {table_namePGI} ({table_columnsPGI})'
                        cursor.execute(create_table_sql_PGI)
                        logging.info('PGI Table has been successfully created!!') 

                        try:
                            ## IMPORT DATA ##
                            create_statement = fts.fast_to_sql(Final_HV_sharedata, "DMS_Proforma", cnxn, if_exists="append", temp=False)
                            new_row = {'Status': 'Proforma data Loaded to DB Successfully', 'Total_Rows_Uploaded':len(Final_HV_sharedata),'Timestamp': today}
                            logging.info('Proforma data Loaded to DB Successfully!!') 
                            cnxn.commit()


                            create_statement_PGI = fts.fast_to_sql(Final_HV_sharedataPGI, "DMS_PGI", cnxn, if_exists="append", temp=False)
                            new_row_PGI = {'Status': 'PGI data Loaded to DB Successfully', 'Total_Rows_Uploaded': len(Final_HV_sharedataPGI), 'Timestamp': today}
                            logging.info('PGI data Loaded to DB Successfully!!') 
                            cnxn.commit()
                        except (pyodbc.Error, pyodbc.Warning, Exception) as e:
                            logging.info("Error:", e)
                            traceback.logging.info_exc()
                            cnxn.rollback()
                            new_row = {'Status': str(e), 'Total_Rows_Uploaded':'','Timestamp': today}
                            
                        with cnxn1.cursor() as cursor1:
                            cursor1.execute("INSERT INTO [dbo].[DMS_Proforma_Logs] (Status,Total_Rows_Uploaded,Timestamp) VALUES (?, ?, ?)", new_row['Status'],new_row['Total_Rows_Uploaded'],new_row['Timestamp'])
                            logging.info("Proforma Status Updated to Logs successfully!!!")
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