import logging
import azure.functions as func
import pyodbc
from azure.storage.blob import BlobServiceClient
import os
import json

#GET APPLICATION VARIABLES
storage_connection_string = os.environ['AzureWebJobsStorage']
blob_container_name = os.environ['SQL_BLOB']
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
blob_name = 'DMS_CT_Main_query.sql'
conn_str = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
conn = pyodbc.connect(conn_str)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        # CHECK FOR BOOKING NUMBER PARAMETER
        req_body = req.get_json()  # Parse the JSON body
        booking_number = req_body.get('booking_number')
        
        if not booking_number:
            logging.info("No booking number provided.")
            return func.HttpResponse("Please provide a booking number parameter.", status_code=400)
        else:
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
            blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)

            # DOWNLOAD QUERY FROM BLOB
            query_bytes = blob_client.download_blob().readall()
            query = query_bytes.decode('utf-8')

            # Modify the query to include the booking number parameter
            modified_query = query + f" WHERE H.booking_number = '{booking_number}'"

            cursor = conn.cursor()
            cursor.execute(modified_query)
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description] 

            # Convert Decimal and datetime.date values
            converted_results = []
            for row in result:
                converted_row = list(row)
                converted_row[6] = float(converted_row[6])  # Convert Decimal to float
                converted_results.append(tuple(converted_row))

            response = []
            for row in converted_results:
                row_dict = {column_names[i]: value for i, value in enumerate(row)}
                response.append(row_dict)
            
            logging.info("Query executed successfully and data pushed to API.")
            logging.info(response)
            
            return func.HttpResponse(
                body = json.dumps(response), 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)