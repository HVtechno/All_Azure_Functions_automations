import logging
import pyodbc
import json
import os
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #GET ENV Application variable
    CORS_URLS = os.environ['CORS_URLs'].split(',')
    DB_DRIVER = os.environ['DB_DRIVER']
    DB_SERVER = os.environ['DB_SERVER']
    DB_DATABASE = os.environ['DB_DATABASE']
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']

    # Establish a database connection
    Hconn = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
    #logging.info("DB_connection_string" , Hconn)
    connection = pyodbc.connect(Hconn)

    # Create a cursor object
    cursor = connection.cursor()

    # Execute the SQL query
    query = """
    SELECT DISTINCT 
    UN.country_name AS value,
    UN.country_name AS label
    FROM [dbo].[MD_Customer_List] AS CL 
    JOIN dbo.MD_Un_Locode as UN on CL.ship_to_country = UN.alpha_2code 
    WHERE customer_status = 'Active' OR customer_status = 'Inactive' 
    ORDER BY country_name
    """
    cursor.execute(query)

    # Fetch all the rows from the query result
    rows = cursor.fetchall()

    # Create a list to store the JSON objects
    output = []

    # Iterate over the rows and create JSON objects
    for row in rows:
        value = row.value
        label = row.label
        output.append('{{"value": "{}", "label": "{}"}}'.format(value, label))

    #Convert the output list to JSON
    json_output = '[{}]'.format(','.join(output))   
    json_output = json_output.replace("//",'')

    #logging the JSON output
    logging.info(json_output)
    return func.HttpResponse(
            body=json_output,
            status_code=201,
            mimetype="application/json"
        )