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
            # DEFINE WHERE CLAUSE CONDITION
            where_clause = []

            if req_body["region"]:
                where_clause.append("Region IN ({})".format(",".join(["'{}'".format(region) for region in req_body["region"]])))
            if req_body["statusSelected"]:
                where_clause.append("SISentStatus IN ({})".format(",".join(["'{}'".format(sentStatus) for sentStatus in req_body["statusSelected"]])))
            if req_body["destinationCountry"]:
                where_clause.append("shiptocountry IN ({})".format(",".join(["'{}'".format(country) for country in req_body["destinationCountry"]])))
            if req_body["customerName"]:
                where_clause.append("ShiptoID IN ({})".format(",".join(["'{}'".format(customer) for customer in req_body["customerName"]])))

            # COMBINE THE WHERE CONDITIONS WITH 'AND'
            where_condition = " AND ".join(where_clause)

            # CHECK THE LENGTH OF ACTIVEARCHIVED LIST AND ADD CONDITIONS ACCORDINGLY
            if len(req_body["activeArchived"]) == 2:
                where_condition = "{} AND DateDocumentSent = '' AND Status IN ('Not Started','In Progress') OR {} AND DateDocumentSent <> '' AND Status IN ('Not Started','In Progress')".format(where_condition,where_condition)
            elif "Active" in req_body["activeArchived"]:
                where_condition = "{} AND DateDocumentSent = '' AND Status IN ('Not Started','In Progress')".format(where_condition)
            elif "Archived" in req_body["activeArchived"]:
                where_condition = "{} AND DateDocumentSent <> '' AND Status IN ('Not Started','In Progress')".format(where_condition)
            elif len(req_body["activeArchived"]) == 0:
                where_condition = "{} AND Status IN ('Not Started','In Progress')".format(where_condition)

            # RUN THE SQL QUERY
            query = """
                    SELECT [ShiptoID],
                        [UniqueID],
                        [ChangedColumn],
                        [OldValue],
                        [NewValue],
                        [Status] 
                    FROM DMS_alert_changes
                    WHERE {}
                    """.format(where_condition)
            logging.info("Formatted Sql query {}".format(query))
            cursor.execute(query)
            # FETCH THE COLUMN NAMES
            column_names = [column[0] for column in cursor.description]

            # FETCH ALL ROWS AND CONVERT INTO DICTIONARIES
            results = [dict(zip(column_names, row)) for row in cursor.fetchall()]
            logging.info(results)
            
            return func.HttpResponse(
                body = json.dumps(results), 
                mimetype="application/json", 
                status_code=200)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.info(error_message)
        return func.HttpResponse(error_message, status_code=500)