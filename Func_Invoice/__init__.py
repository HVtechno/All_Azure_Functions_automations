import logging
import json
import azure.functions as func
from datetime import datetime

def main(req: func.HttpRequest, InvoiceItems: func.Out[func.SqlRow]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request and ready to insert data in SQL DB')
    req_body = req.get_json()
    # Create a set of unique IDs to check for duplicates
    unique_ids = set()
    counter = 1 
    for items in req_body:
        items['Date_Created'] = items.get('Date_Created', None)
        items['Date_Download'] = items.get('Date_Download', None)
        items['File_Name'] = items.get('File_Name',None)
        items['Invoice_Amount'] = items.get('Invoice_Amount',None)
        items['Invoice_Number'] = items.get('Invoice_Number',None)
        items['Invoice_Quantity'] = items.get('Invoice_Quantity',None)
        items['PO_Number'] = items.get('PO_Number',None)
        items['Sales_Organization'] = items.get('Sales_Organization',None)
        items['Ship_To_Id'] = items.get('Ship_To_Id',None)
        items['Delivery'] = items.get('Delivery',None)
        items['Material'] = items.get('Material',None)
        items['Output'] = items.get('Output',None)
        items['Shipment_Number'] = items.get('Shipment_Number',None)
        # Get current date and time
        Current_now = datetime.now()
        Current_Time = Current_now.strftime("%d%m%H%M%S") 
        if items['Sales_Document'] in unique_ids:
            # If the Sales_document is a duplicated, assign 1st sales_doc json body with 1 & next with increment value
            items['Dup_Value'] = counter + 1
            counter += 1
            items['UniqueValue'] = str(items['Sales_Document']) + "-" + Current_Time + "-" + str(items['Dup_Value'])
        else:
            # If the sales_document is unique, assign a value of 1 and add to the set of unique IDs
            items["Dup_Value"] = 1
            unique_ids.add(items["Sales_Document"])
            items['UniqueValue'] = str(items['Sales_Document']) + "-" + Current_Time + "-" + str(items['Dup_Value'])
        
        # sql_rows = func.SqlRowList(map(lambda r: func.SqlRow.from_dict(r), [items]))
        # InvoiceItems.set(sql_rows)

    sql_rows = func.SqlRowList(map(lambda r: func.SqlRow.from_dict(r), req_body))
    InvoiceItems.set(sql_rows)
        
    logging.info(json.dumps(req_body))
    return func.HttpResponse(
            body=json.dumps(req_body),
            status_code=201,
            mimetype="application/json"
        )