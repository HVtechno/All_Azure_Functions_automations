import logging
import azure.functions as func
import pyodbc
import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient

#GET APPLICATION VARIABLES
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
storage_connection_string = os.environ['AzureWebJobsStorage']
container_name = os.environ['SQL_BLOB']
blob_file_name = os.environ['DMS_YEARLY_CERT_QUERY']

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file_name)
query = blob_client.download_blob().readall().decode('utf-8')

#CONNECTION TO DATABASE
conn_str = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
            cursor.execute(query)
            # Fetch all rows at once
            rows = cursor.fetchall()
            
            # FETCH THE COLUMN NAMES
            column_names = [column[0] for column in cursor.description]

            # FETCH ALL ROWS AND CONVERT INTO DICTIONARIES
            results = [dict(zip(column_names, row)) for row in rows if row[column_names.index('Status')] != 'Completed']
            logging.info(results)
            
            return func.HttpResponse(
                body = json.dumps(results,indent=4), 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)