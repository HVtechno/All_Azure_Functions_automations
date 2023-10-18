import datetime
import logging
import azure.functions as func
import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts
import warnings
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

#GET ALL APPLICATION VARIABLES
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
MEC_Query = os.environ['MEC_BLOB_QUERY']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']

#DB CONNECTION
Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
#logging.info(Hconn)
cnxn = pyodbc.connect(Hconn)
cnxn1 = pyodbc.connect(Hconn)

#BLOB CONNECTION
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
# Get the container client
container_client = blob_service_client.get_container_client(container_name)
# Get the blob client for the query file
blob_client = container_client.get_blob_client(MEC_Query)
# Download the query file as text
MEC_query_text = blob_client.download_blob().readall().decode("utf-8")

#Datetime
today = datetime.datetime.today()

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    with cnxn.cursor() as cursor:
        try:
            cnxn.execute("SELECT 1")
            logging.info("Database Connected successfully!!!")
            # DEL_share = '''IF OBJECT_ID('DMS_MEC_Report', 'U') IS NOT NULL
            #                     DROP TABLE DMS_MEC_Report'''
            DEL_share = '''TRUNCATE TABLE DMS_MEC_Report'''
            cursor.execute(DEL_share)
            logging.info('MEC Table "DMS_MEC_Report" dropped successfully!!')

            # MEC_Report = pd.read_sql_query(MEC_query_text,cnxn)
            # logging.info(len(MEC_Report))
            # MEC_Report_DF = pd.DataFrame(MEC_Report)
            # MEC_Report_DF['export_zone'] = MEC_Report_DF['export_zone'].astype(str)
            # MEC_Report_DF['supply_zone'] = MEC_Report_DF['supply_zone'].astype(str)
            # MEC_Report_DF['customer_purchase_order_number'] = MEC_Report_DF['customer_purchase_order_number'].astype(str)
            # MEC_Report_DF['source'] = MEC_Report_DF['source'].astype(str)
            # MEC_Report_DF['sales_order_number'] = MEC_Report_DF['sales_order_number'].astype(str)
            # MEC_Report_DF['shipment_number'] = MEC_Report_DF['shipment_number'].astype(str)
            # MEC_Report_DF['delivery_number'] = MEC_Report_DF['delivery_number'].astype(str)

            # MEC_Report_DF['zone_delivery_pgi_date'] = pd.to_datetime(MEC_Report_DF['zone_delivery_pgi_date'],errors='coerce')
            # MEC_Report_DF['zone_delivery_pgi_date'] = MEC_Report_DF['zone_delivery_pgi_date'].dt.strftime('%Y-%m-%d')
        
            # MEC_Report_DF['sales_organization'] = MEC_Report_DF['sales_organization'].astype(str)
            # MEC_Report_DF['importer'] = MEC_Report_DF['importer'].astype(str)
            # MEC_Report_DF['ship_to_country'] = MEC_Report_DF['ship_to_country'].astype(str)
            # MEC_Report_DF['ship_to_name'] = MEC_Report_DF['ship_to_name'].astype(str)
            # MEC_Report_DF['booking_number'] = MEC_Report_DF['booking_number'].astype(str)
            # MEC_Report_DF['invoice_number'] = MEC_Report_DF['invoice_number'].astype(str)

            # MEC_Report_DF['billing_date'] = pd.to_datetime(MEC_Report_DF['billing_date'],errors='coerce')
            # MEC_Report_DF['billing_date'] = MEC_Report_DF['billing_date'].dt.strftime('%Y-%m-%d')

            # MEC_Report_DF['booking_status'] = MEC_Report_DF['booking_status'].astype(str)
            # MEC_Report_DF['automation_status'] = MEC_Report_DF['automation_status'].astype(str)
            # MEC_Report_DF['invoice_status'] = MEC_Report_DF['invoice_status'].astype(str)
            # MEC_Report_DF['status_good_receipt'] = MEC_Report_DF['status_good_receipt'].astype(str)
            # MEC_Report_DF['good_receipt_qty'] = MEC_Report_DF['good_receipt_qty'].astype(float)
            # MEC_Report_DF['quantity_delivery'] = MEC_Report_DF['quantity_delivery'].astype(float)
            # MEC_Report_DF['invoice_receipt_qty'] = MEC_Report_DF['invoice_receipt_qty'].astype(float)
            # MEC_Report_DF['pgi_ir'] = MEC_Report_DF['pgi_ir'].astype(str)
            # MEC_Report_DF['prepayment'] = MEC_Report_DF['prepayment'].astype(str)
            # MEC_Report_DF['transport'] = MEC_Report_DF['transport'].astype(str)
            # MEC_Report_DF['po_real'] = MEC_Report_DF['po_real'].astype(str)

            # MEC_Report_DF['invoiced_hl'] = MEC_Report_DF['invoiced_hl'].astype(float)
            # MEC_Report_DF['invoice_value'] = MEC_Report_DF['invoice_value'].astype(float)
            # MEC_Report_DF['SKU'] = MEC_Report_DF['SKU'].astype(str)
            # MEC_Report_DF.replace(['nan','None'], '', inplace=True)

            # create_statement = fts.fast_to_sql(MEC_Report_DF, "DMS_MEC_Report", cnxn, if_exists="append", temp=False)
            cursor.execute(MEC_query_text)
            cnxn.commit()
            logging.info("Data loaded successfully!!!")
            cursor2 = cnxn.cursor()
            cursor2.execute("SELECT COUNT(*) FROM dbo.DMS_MEC_REPORT")
            row_count = cursor2.fetchone()[0]
            new_row = {'Status': 'MEC Data Loaded to DB Successfully','Total_Rows_Available': str(row_count),'Timestamp': today}

        except pyodbc.Error as e:
            cnxn.rollback()
            logging.info("Data loading failed: %s", str(e))
            new_row = {'Status': 'MEC data Failed to loaded. Please check','Total_Rows_Available': '','Timestamp': today}
        
        with cnxn1.cursor() as cursor1:
            cursor1.execute("SELECT 1")
            logging.info("Logs Database Connected successfully!!!")
            cursor1.execute("INSERT INTO [dbo].[DMS_MEC_Report_Logs] (Status,Total_Rows_Available,Timestamp) VALUES (?, ?, ?)", new_row['Status'],new_row['Total_Rows_Available'],new_row['Timestamp'])
            cnxn1.commit()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)