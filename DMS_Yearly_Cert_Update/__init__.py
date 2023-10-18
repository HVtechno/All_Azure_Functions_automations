import logging
import azure.functions as func
import pyodbc
import os
import json
from azure.storage.blob import BlobServiceClient, BlobClient
import pandas as pd

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
        # CHECK FOR VALID JSON
        req_body = req.get_json() 
        
        if not req_body:
            logging.info("There is no valid JSON body available")
            return func.HttpResponse("Please provide a valid JSON body", status_code=400)
        else:
            # Define the update query
            update_query = """
                UPDATE [dbo].[DMS_Yearly_Cert]
                SET 
                    DocumentName = ?,
                    DocumentDescription = ?,
                    Legalization = ?,
                    LatestDocumentDate = CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CONVERT(DATE, ?, 23) END,
                    ExpirationDate = CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CONVERT(DATE, ?, 23) END,
                    AlertDate = CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CONVERT(DATE, ?, 23) END,
                    Status = ?,
                    Comment = ?
                WHERE ID = ?
            """

            # Execute the update query with parameterized values
            cursor.execute(update_query, (req_body['DocumentName'], 
                                        req_body['DocumentDescription'],
                                        req_body['Legalization'],
                                        req_body['LatestDocumentDate'],
                                        req_body['LatestDocumentDate'],
                                        req_body['LatestDocumentDate'],
                                        req_body['ExpirationDate'],
                                        req_body['ExpirationDate'],
                                        req_body['ExpirationDate'],
                                        req_body['AlertDate'],
                                        req_body['AlertDate'],
                                        req_body['AlertDate'],
                                        req_body['Status'],
                                        req_body['Comment'],
                                        req_body['ID']))

            conn.commit()

            #Run the database Query and Fetch the result
            df_yearlycert = pd.read_sql_query(query,conn)
            df_yearlycert_final = pd.DataFrame(df_yearlycert)
            if len(df_yearlycert_final) > 0:
                df_yearlycert_agg = df_yearlycert_final.groupby(
                    ['country', 'ShipTo', 'Region', 
                    'SoldTo','payer','shipping',
                    'customer_status','ID','DocumentName','DocumentDescription',
                    'Legalization','LatestDocumentDate','ExpirationDate',
                    'AlertDate','Status','Comment'])['sales_organization'].apply(';'.join).reset_index()

            # Convert the DataFrame to a list of dictionaries
            data = df_yearlycert_agg.to_dict(orient='records')

            # Convert the result to JSON
            json_result = json.dumps(data, indent=4)

            return func.HttpResponse(
                body = json_result, 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)