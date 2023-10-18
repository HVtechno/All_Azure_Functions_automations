import logging
import azure.functions as func
import pyodbc
import os

#GET APPLICATION VARIABLES
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']

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
            Insert_query = """
            INSERT [dbo].[DMS_Yearly_Cert]
            (Country, 
            ShipTo, 
            DocumentName, 
            DocumentDescription, 
            Legalization, 
            LatestDocumentDate, 
            ExpirationDate,
            AlertDate,
            Status,
            Comment)
            Values (?,?,?,?,?,?,?,?,?,?)
            """

            # Execute the update query with parameterized values
            cursor.execute(Insert_query, (req_body['Country'], 
                                        req_body['ShipToID'], 
                                        req_body['DocumentName'], 
                                        req_body['DOcumentDescrption'],
                                        req_body['Legalization'],
                                        req_body['LastDocumentDate'],
                                        req_body['ExpirationDate'],
                                        req_body['AlertDate'],
                                        req_body['Status'],
                                        req_body['Comment']))

            conn.commit()

            return func.HttpResponse(
                body = "Request certificate successfully posted to DMS", 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)