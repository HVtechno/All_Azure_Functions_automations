import datetime
import logging
import azure.functions as func
import pandas as pd
import pyodbc
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from fast_to_sql import fast_to_sql as fts

#GET APPLICATION VARIABLES SETTING
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
connection_string = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
blob_file_name = os.environ['BREW_DATA_MISMATCH_QUERY']
blob_file_name_2 = os.environ['BREW_DATA_CHANGES_QUERY']

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file_name)
blob_client_2 = blob_service_client.get_blob_client(container=container_name, blob=blob_file_name_2)
query = blob_client.download_blob().readall().decode('utf-8')
query_2 = blob_client_2.download_blob().readall().decode('utf-8')

#DEFINE SQL CONNECTION FUNCTION
def connect_to_sql():
    try:
        cnxn = pyodbc.connect(connection_string)
        return cnxn
    except pyodbc.Error as e:
        logging.info("Error connecting to SQL Server: %s", str(e))
        raise

#DEFINE MOVE D0 DATA to HIST FUNCTION
def Move_D0_hist(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
                    IF OBJECT_ID('DMS_alert_hist', 'U') IS NOT NULL 
                            DROP TABLE DMS_alert_hist
                    
                    SELECT * 
                    INTO [dbo].[DMS_alert_hist]
                    FROM [dbo].[DMS_alert_D0]
                    ''')
        conn.commit()
        logging.info("Data successfully moved from DMS_alert_D0 to DMS_alert_his")

#DEFINE CREATING ALERTS FOR D0 FUNCTION
def Alert_D0(conn):
    #Run SQL Query
    df_shipment_D0 = pd.read_sql_query(query,conn)
    #create dataframe
    shipment_D0 = pd.DataFrame(df_shipment_D0)
    shipment_D0['Containersloaded'] = shipment_D0['Containersloaded'].astype(str)

    #Group all rows with sorted values based upon columns ['ShiptoID','UniqueID']
    result_df = shipment_D0.fillna('').groupby(['ShiptoID','UniqueID']).agg({
                                                                 'Bookingnumber': lambda x: ';'.join(sorted(set(x))), 
                                                                 'POnumber': lambda x: ';'.join(sorted(set(x))), 
                                                                 'POL': lambda x: ';'.join(sorted(set(x))),
                                                                 'Portofdestination': lambda x: ';'.join(sorted(set(x))), 
                                                                 'Vesselname': lambda x: ';'.join(sorted(set(x))), 
                                                                 'CarrierMoveType': lambda x: ';'.join(sorted(set(x))),
                                                                 'Containersloaded': lambda x: ';'.join(sorted(set(x))),
                                                                 'Containernumber': lambda x: ';'.join(sorted(set(x))),
                                                                 'Sealnumber': lambda x: ';'.join(sorted(set(x))),
                                                                 'GrossWeight': lambda x: ';'.join(sorted(set(x))),
                                                                 'NetWeight': lambda x: ';'.join(sorted(set(x))),
                                                                 'SKU+Description': lambda x: ';'.join(sorted(set(x))),
                                                                 'Region': lambda x: ';'.join(sorted(set(x))),
                                                                 'shiptocountry': lambda x: ';'.join(sorted(set(x))),
                                                                 'ShiptoNumber': lambda x: ';'.join(sorted(set(x))),
                                                                 'DateDocumentSent': lambda x: ';'.join(sorted(set(x))),
                                                                 'SISentStatus': lambda x: ';'.join(sorted(set(x)))
                                                                 }).reset_index()

    #Drop all NA or blank rows from column Unique_ID
    Final_result = result_df[result_df['UniqueID'].str.strip().str.len() > 0]

    #Finally Arrange data on alphabatical order based on "UniqueID"
    Final_result_sorted = Final_result.sort_values(by='UniqueID', ascending=True)

    #get all columns
    column_names = Final_result_sorted.columns.tolist()

    #Create Table & Upload data to Table
    with conn.cursor() as cursor:
        cursor.execute("IF OBJECT_ID('DMS_alert_D0', 'U') IS NOT NULL DROP TABLE DMS_alert_D0")
        logging.info("Table DMS_alert_D0 is dropped")
        create_table_query = f'''
        CREATE TABLE DMS_alert_D0 (
            {', '.join([f'[{col}] VARCHAR(MAX)' for col in column_names])}
        )
        '''
        cursor.execute(create_table_query)
        create_statement = fts.fast_to_sql(Final_result_sorted, "DMS_alert_D0", conn, if_exists="append", temp=False)
        conn.commit()
        logging.info("Table DMS_alert_D0 created successfully")
        logging.info("Data loaded to DMS_alert_D0 successfully!!!")

#DEFINE OVERALL CHANGES BETWEEN D0 AND HIST FUNCTION
def Alert_Changes(conn):
    # run sql query
    df_shipment_changes = pd.read_sql_query(query_2,conn)

    #create a dataframe
    Final_changes = pd.DataFrame(df_shipment_changes)

    if len(Final_changes) > 0:
        with conn.cursor() as cursor:
            create_statement = fts.fast_to_sql(Final_changes, "DMS_alert_changes", conn, if_exists="append", temp=False)
            conn.commit()
            logging.info("There are changes identified and loaded to table DMS_alert_changes successfully!!!")
    else:
        logging.info('There are no changes identified today')

#FINALLY MAIN FUNCTION
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    SQLconn = connect_to_sql()
    logging.info('Database is connected successfully')
    Move_D0_hist(SQLconn)
    logging.info('Data is moved to DHist')
    Alert_D0(SQLconn)
    logging.info('Data is moved to D0')
    Alert_Changes(SQLconn)
    
    logging.info('Python timer trigger function ran at %s', utc_timestamp)