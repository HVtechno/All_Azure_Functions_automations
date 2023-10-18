import datetime
import logging
import delta_sharing
import os
# import pandas as pd
# import io, json

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')


    # Set the Delta Sharing server URL and token
    profile = os.environ['DELTASHARE_CONFIG']
    client = delta_sharing.SharingClient(profile)
    logging.info('Client Set')

    shares = client.list_shares()
    
    for share in shares:
        schemas = client.list_schemas(share)
        for schema in schemas:
            tables = client.list_tables(schema)
            for table in tables:
                logging.info(f'name = {table.name}, share = {table.share}, schema = {table.schema}')


    table_url = profile + "az.countries_eu_az"
    df = delta_sharing.load_as_pandas(url=table_url, limit=1)

    logging.info(df['country_alpha_3_code'])

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
