"""
Script to join and do prelimenary cleaning of the raw data,
removing double id's and replacing id's with names where possible
"""

import pandas as pd
import numpy as np

from functions import *

# Brum project
PROJECT_ID = 'brum-dev-b72f'
DATASET_BRONZE = 'tiltak_bronze'
SA_KEY_NAME = 'service-account-key'

# Create client based on brum-dev
bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

df_tiltakstyper = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'tiltakstyper_bronze')
df_gjennomforing = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'gjennomforinger_bronze')
df_gjennomforing_enhet = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'nav_enhet_bronze')



print(df_tiltakstyper.info())
print(df_gjennomforing.info())
print(df_gjennomforing_enhet.info())

