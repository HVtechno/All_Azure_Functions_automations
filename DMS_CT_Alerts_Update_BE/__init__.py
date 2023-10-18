import logging
import azure.functions as func
import pyodbc
import os
import json

#GET APPLICATION VARIABLES
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
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
            # RUN THE SQL QUERY
            query = """
                UPDATE [dbo].[DMS_alert_changes]
                SET [Status] = ?
                WHERE [ShiptoID] = ? AND [UniqueID] = ? AND [ChangedColumn] = ?
                """
            # EXECUTE QUERY WITH PARAMETERIZED VALUES
            cursor.execute(query, (req_body['Status'], req_body['ShiptoID'], req_body['UniqueID'], req_body['ChangedColumn']))
            conn.commit()

            return func.HttpResponse(
                body = "Status {} Updated for the column {} with respective of UniqueID {}".format(req_body['Status'],req_body['ChangedColumn'],req_body['UniqueID']), 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)