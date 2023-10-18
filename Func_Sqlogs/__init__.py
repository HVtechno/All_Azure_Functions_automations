import logging
import json
import azure.functions as func
from datetime import datetime
import random

def main(req: func.HttpRequest, SqlogItems: func.Out[func.SqlRow]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request and ready to insert data in SQL DB')
    req_body = req.get_json()
    
    # Create a set of unique IDs to check for duplicates
    unique_ids = set()
    counter = 1 
    
    for items in req_body:

        items['Sales_Area'] = items.get('Sales_Area',None)
        items['SAP_Shipment'] = items.get('SAP_Shipment',None)
        items['Error_Message'] = items.get('Error_Message',None)
        items['TimeStamp'] = items.get('TimeStamp',None)
        
        # Get current date and time
        Current_now = datetime.now()
        Current_Time = Current_now.strftime("%d%m%H%M%S")
        
        if 'Sales_Document' in items:
            id1 = str(items['Sales_Document'])
        elif 'SAP_Shipment' in items:
            id1 = str(items['SAP_Shipment'])
        else:
            id1 = str(random.randint(10000,99999))

        if id1 in unique_ids:
            # If the Sales_document is a duplicated, assign 1st sales_doc json body with 1 & next with increment value
            items['Dup_Value'] = counter + 1
            counter += 1
            items['UniqueValue'] = id1 + "-" + Current_Time + "-" + str(items['Dup_Value'])
        else:
            # If the sales_document is unique, assign a value of 1 and add to the set of unique IDs
            items["Dup_Value"] = 1
            unique_ids.add(id1)
            items['UniqueValue'] = id1 + "-" + Current_Time + "-" + str(items['Dup_Value'])
        
    sql_rows_logs = func.SqlRowList(map(lambda r: func.SqlRow.from_dict(r), req_body))
    SqlogItems.set(sql_rows_logs)


    logging.info(json.dumps(req_body))
    return func.HttpResponse(
            body=json.dumps(req_body),
            status_code=201,
            mimetype="application/json"
        )
