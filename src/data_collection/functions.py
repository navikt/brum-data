import pandas as pd
import numpy as np
import json
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import secretmanager

def _get_secret(project_id, sa_key_name='service-account-key'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{sa_key_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = json.loads(response.payload.data.decode('UTF-8'))
    return secret

def create_client(project_id, sa_key_name):
    key = _get_secret(project_id, sa_key_name)
    creds = service_account.Credentials.from_service_account_info(key)
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    return client

def write_to_BQ(client, table_name, dframe, dataset='Kaggle_test_data', disp = "WRITE_APPEND", schema_name='schema.json'):
    with open(schema_name, 'rb') as f:
        schema = json.load(f)

    table_id = dataset+'.'+table_name

    job_config = bigquery.LoadJobConfig(
        schema=schema[table_name],
        write_disposition = disp
    )

    job = client.load_table_from_dataframe(
        dframe, table_id, job_config=job_config
    )

def get_data_from_BQ(bq_client, TARGET_PROJECT_ID, DATASET, source_table):
    # Read data from BigQuery
    sql_src_qry = f"SELECT * FROM `{TARGET_PROJECT_ID}.{DATASET}.{source_table}`"

    df = bq_client.query(sql_src_qry).to_dataframe()
    df.replace({np.nan: None}, inplace = True)

    return df

def update_raw_data(client, query):
    logging.info(f"Attempting to get raw data from BQ")
    try:
        client.query(query)
        logging.info("Query handled successfully")
    except Exception as e:
        logging.error(f"Query failed: {e}")