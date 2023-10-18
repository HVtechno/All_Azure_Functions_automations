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
    UN.country_name AS country,
    CL.ship_to_name + '-' + CL.ship_to AS value,
    CL.ship_to_name + '-' + CL.ship_to AS label
    FROM [dbo].[MD_Customer_List] AS CL
    JOIN dbo.MD_Un_Locode AS UN ON CL.ship_to_country = UN.alpha_2code
    WHERE customer_status = 'Active' OR customer_status = 'Inactive'
    GROUP BY UN.country_name, CL.ship_to, cl.ship_to_name
    ORDER BY country_name, value DESC
    """
    cursor.execute(query)

    # Fetch all the rows from the query result
    rows = cursor.fetchall()

    # Create a dictionary to store the country and its values
    country_data = {}

    # Iterate over the rows and create JSON objects
    for row in rows:
        country = row.country
        value = row.value
        label = row.label
        if value is not None:
            if '"' in value:
                value = value.replace('"', '')

        if label is not None:
            if '"' in label:
                label = label.replace('"', '')
        if country in country_data:
            country_data[country].append({"value": value, "label": label})
        else:
            country_data[country] = [{"value": value, "label": label}]

    # Convert the country_data dictionary to the desired output format
    output = []
    for country, values in country_data.items():
        #output.append('{}: {}'.format(country, json.dumps(values, ensure_ascii=False)))
        formatted_values = []
        for entry in values:
            formatted_values.append(f'{{"value": "{entry["value"]}", "label": "{entry["label"]}"}}')
        output.append(f'"{country}": [{", ".join(formatted_values)}]')
        
    # Join the output list with commas
    json_output = ', '.join(output)
    json_final = json_output.replace("--","-")

    # Logging the JSON output
    logging.info(json_final)
    return func.HttpResponse(
        body='{{{}}}'.format(json_final),
        status_code=201,
        mimetype="application/json"
    )