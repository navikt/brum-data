# Get_data
import pandas as pd
import numpy as np

from functions import create_client, write_to_BQ, get_data_from_BQ

# Project IDs, Keys, Datasets and source tables for all necessary BQ databases
data_projects = [
    {
        'SOURCE_PROJECT_ID': 'brum-dev-b72f',
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

# Create client based on brum-dev
bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

# Requests the data from BiqQuery and returns a dataframe

df_tiltakstyper = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'tiltakstype_view')
df_gjennomforing = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_view')
df_gjennomforing_enhet = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_nav_enhet_view')

# Animal crossing test
#df, bq_client_animal = get_data_from_BQ(PROJECT_ID, SA_KEY_NAME, DATASET, source_table)
#df_subset = df[['Name', 'UniqueEntryID']]

print(df_tiltakstyper.info())
print(df_gjennomforing.info())
print(df_gjennomforing_enhet.info())
#print(df_subset.head())

#write_to_BQ(client=bq_client_animal, table_name="animal_crossing_fossils", dframe=df_subset, dataset=DATASET, disp = "WRITE_TRUNCATE")

