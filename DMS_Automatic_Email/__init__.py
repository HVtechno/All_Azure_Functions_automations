import pyodbc
import requests
import json
import msal
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.files.file import File
import os
from io import BytesIO
import base64
import datetime
import logging
import azure.functions as func
import time

# DECLARE ALL APPLICATION VARIABLE
DB_DRIVER = os.environ['DB_DRIVER']
DB_SERVER = os.environ['DB_SERVER']
DB_DATABASE = os.environ['DB_DATABASE']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
client_id = os.environ['EMAIL_CLIENT_ID']
client_secret = os.environ['EMAIL_CLIENT_SECRET']
tenant_id = os.environ['EMAIL_TENENT_ID']
username = os.environ['SP_USERNAME']
password = os.environ['SP_PASSWORD']
site_url = os.environ['SP_SITE']
send_mailbox = os.environ['SEND_MAILBOX']
today = datetime.datetime.today().strftime('%d/%m/%Y')
max_attempts = 5
retry_delay = 3


def connect_to_db():
    try:
        connection_string = "Driver=" + "{" + DB_DRIVER + "};" + "Server=" + DB_SERVER + ";" + "Database=" + DB_DATABASE + ";" + "uid=" + DB_USERNAME + ";" + "pwd=" + DB_PASSWORD + ";" + "Trusted_Connection = yes;"
        conn = pyodbc.connect(connection_string)
        logging.info("Database connection successful.")
        return conn
    except pyodbc.Error as e:
        # Raise an exception and exit the function if there's an error during the connection
        raise Exception(f"Error connecting to the database: {e}")

def fetch_ready_rows(connection):
    try:
        # Execute the query to fetch rows with "Ready" status
        query = '''
                SELECT DISTINCT
                sales_order_number
                ,unique_item_id
                ,CASE 
                    WHEN H.shipping = 'Ocean' THEN 'Booking number'
                    WHEN H.shipping = 'Truck' THEN 'PO number'
                END AS 'Booking/PO_name'
                ,[export_zone_flow]
                ,H.[shipping]
                ,[subject]
                ,[documentation_agent]
                ,CASE
                    WHEN RIGHT(Replace([customer_contact],' ',''), 1) = ';' THEN
                        LEFT(Replace([customer_contact],' ',''), LEN(Replace([customer_contact],' ','')) - 1)
                    ELSE
                        Replace([customer_contact],' ','')
                END AS [customer_contact]
                ,concat('Shared%20Documents','/',[folder_path]) as 'folder_path'
                ,[send_email_comment]
                FROM [dbo].[Send_Email] as V
                LEFT JOIN [dbo].[MD_Shipping_Condition] AS H ON H.shipping_condition = V.shipping_conditions
                WHERE send_email_status = 'Ready' and customer_contact <> '-'
                '''
        cursor = connection.cursor()
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()

        # Check if rows exist, if not, return None
        if not rows:
            logging.info("There are no rows to retrieve. Aborting the process.")
            return
        
        # Get the column names from the cursor description
        columns = [column[0] for column in cursor.description]
        
        # Create a list of dictionaries where keys are column names and values are row values
        rows_as_dicts = [dict(zip(columns, row)) for row in rows]
        
        # Return the result set as a list of dictionaries
        return rows_as_dicts
        
    except pyodbc.Error as e:
        # Raise an exception if there's an error during the query execution
        raise Exception(f"Error fetching rows from the database: {e}")
    
def send_email(customer_contact, file_content, subject, agent, Book_PO, Book_PO_Name, file_name, Hconn):
    authority = f"https://login.microsoftonline.com/{tenant_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority)

    scopes = ["https://graph.microsoft.com/.default"]

    result = None
    result = app.acquire_token_silent(scopes, account=None)

    if not result:
        result = app.acquire_token_by_username_password(username, password, scopes=scopes)
        # logging.info(result)
        logging.info("successfully retrieved Access Token, now will send emails")

    if "access_token" in result:
        endpoint = f'https://graph.microsoft.com/v1.0/users/{send_mailbox}/microsoft.graph.sendMail'
        toUserEmail = customer_contact.split(';')
        email_content = f"""
            <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                    }}
                    .email-content {{
                        padding: 20px;
                    }}
                    .signature {{
                        margin-top: 20px;
                        font-style: italic;
                    }}
                </style>
            </head>
            <body>
                <div class="email-content">
                    <p>Dear Customer,</p>
                    <p>Please find attached the AbInBev exports documentation for the {Book_PO_Name} <strong>{Book_PO}</strong>.</p>
                    <p>Please note that if any documents relating to your purchase are incorrect, you must, within 3 business days of receipt of the incorrect document, request a correction by sending an email to <a href="mailto:connect@ab-inbev.com">connect@ab-inbev.com</a> in accordance with section 3d of our Complaints Policy.</p>
                    <p>If you have any questions, please don't hesitate to contact Connect.</p>
                    <p>Have a nice day!</p>
                    
                    <div class="signature">
                        <p>Regards,</p>
                        <p>{agent}</p>
                        <p>ABInBev Exports Documentation Team</p>
                    </div>
                </div>
            </body>
            </html>
            """

        email_msg = {
            'message': {
                'subject': subject,
                'body': {
                    'contentType': 'HTML',
                    'content': email_content,
                },
                'toRecipients': [{'emailAddress': {'address': email}} for email in toUserEmail],
                'attachments': [
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'name': file_name,
                            'contentBytes': base64.b64encode(file_content.read()).decode('utf-8')
                        }
                    ]
            },
            'saveToSentItems': 'true'
        }

        r = requests.post(endpoint, headers={
                        'Authorization': 'Bearer ' + result['access_token']}, json=email_msg)
        
        #get the id of the email and based on the id get status if it was sent. If not sent, then log error, otherwise mark as completed
        
        for attempt in range(1, max_attempts + 1):
            try:
                Hconn = connect_to_db()
                # If the connection is successful, break out of the loop
                break
            except Exception as e:
                print(f"Attempt {attempt} failed: {str(e)}")
                if attempt < max_attempts:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Max attempts reached. Could not establish a connection.")
        
        if r.ok:
            logging.info('Email Sent Successfully' + ' ' + 'for unique_item_id :' + Book_PO)
            query = f"""
                UPDATE [dbo].[Send_Email] 
                SET [send_email_status] = 'Completed', 
                    [date_send_email] = '{today}'
                WHERE [unique_item_id] = '{Book_PO}';
            """
            cursor = Hconn.cursor()
            cursor.execute(query)
            Hconn.commit()
            logging.info('Database Updated with status completed' + ' ' + 'for unique_item_id :' + Book_PO)
        else:
            try:
                response_json = r.json()
                if 'error' in response_json and 'message' in response_json['error']:
                    error_message = response_json['error']['message']
                else:
                    error_message = r.text      
            except Exception as e:
                error_message = str(e)
            error_message = error_message.replace("'", "''")
            
            logging.info(error_message)
            query = f"""
                UPDATE [dbo].[Send_Email] 
                SET [send_email_status] = 'Error', 
                    [send_email_comment] = '{error_message}',
                    [date_send_email] = '{today}'
                WHERE [unique_item_id] = '{Book_PO}';
            """
            cursor = Hconn.cursor()
            cursor.execute(query)
            Hconn.commit()
            logging.info('Database Updated with status error' + ' ' + 'for unique_item_id :' + Book_PO)
    else:
        logging.info(result.get("error"))
        logging.info(result.get("error_description"))
        logging.info(result.get("correlation_id"))
        
def authenticate_sharepoint(username, password, site_url):
    try:
        ctx_auth = AuthenticationContext(site_url)
        ctx_auth.acquire_token_for_user(username, password)
        ctx = ClientContext(site_url, ctx_auth)
        logging.info("Authentication success")
        return ctx
    except Exception as e:
        logging.info(f"An error occurred during SharePoint authentication: {e}")
        return None

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    # Step 1: Connect to the database
    Hconn = connect_to_db()
    # Step 2 : connect to sharepoint and Authenticate with SharePoint using the credentials
    ctx = authenticate_sharepoint(username, password, site_url)
    if Hconn and ctx:    
        # Step 3: Fetch rows with "Ready" status
        ready_rows = fetch_ready_rows(Hconn)
        # logging.info(ready_rows)
        # Step 4: Iterate through the rows and send emails
        if ready_rows is not None:
            for row in ready_rows:
                customer_contact = row['customer_contact']
                folder_paths = row['folder_path']
                subject = row['subject']
                agent = row['documentation_agent']
                Book_PO = row['unique_item_id']
                Book_PO_Name = row['Booking/PO_name']
                folder_url = folder_paths
                
                # Step 5 : Get all files in the SharePoint folder
                folder = ctx.web.get_folder_by_server_relative_url(folder_url)
                files = folder.files
                ctx.load(files)
                ctx.execute_query()

                # Step 6 : Iterate ecah file and check if there is a zip file available
                for file in files:
                    file_name = file.properties["Name"]
                    if file_name.endswith(".zip"):
                    # Step 7: If Zip file exist, copy the contents and send email
                        response = File.open_binary(ctx, file.serverRelativeUrl)
                        if response.status_code == 200 and response.headers.get("Content-Type") == "application/octet-stream":
                            file_content = response.content          
                            # Step 8: Send email using Microsoft Graph API and Update DB with relevant status
                            send_email(customer_contact, BytesIO(file_content), subject, agent, Book_PO, Book_PO_Name, file_name, Hconn)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)