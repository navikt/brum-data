# Get_data
import pandas as pd
import numpy as np

from functions import create_client, write_to_BQ

# Project IDs, Keys, Datasets and source tables for all necessary BQ databases
data_projects = [
    {
        'PROJECT_ID': 'team-mulighetsrommet-prod-5492',
        'SA_KEY_NAME': 'service-account-key',
        'DATASET': 'mulighetsrommet_api_datastream',
        'source_table': 'tiltakstype_view'
    }
]

PROJECT_ID = 'brum-dev-b72f'
SA_KEY_NAME = 'service-account-key'
DATASET = 'Kaggle_test_data'
source_table = 'Fossils'

# Requests the data from BiqQuery and returns a dataframe
def get_data_from_BQ(PROJECT_ID, SA_KEY_NAME, DATASET, source_table):
    # Create BigQuery client
    bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

    # Read data from BigQuery
    sql_src_qry = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{source_table}`"

    df = bq_client.query(sql_src_qry).to_dataframe()
    df.replace({np.nan: None}, inplace = True)

    return df, bq_client

df = get_data_from_BQ(data_projects[0]['PROJECT_ID'], data_projects[0]['SA_KEY_NAME'], data_projects[0]['DATASET'], data_projects[0]['source_table'])

# Animal crossing test
#df, bq_client_animal = get_data_from_BQ(PROJECT_ID, SA_KEY_NAME, DATASET, source_table)
#df_subset = df[['Name', 'UniqueEntryID']]

print(df.head())
print(df.info())
#print(df_subset.head())

#write_to_BQ(client=bq_client_animal, table_name="animal_crossing_fossils", dframe=df_subset, dataset=DATASET, disp = "WRITE_TRUNCATE")

