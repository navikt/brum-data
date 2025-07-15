import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src import Config
from src.data_collection.functions import *
from itertools import product

# Lag en BigQuery klient med brum service account
bq_client = create_client(Config.BRUM_PROJECT_ID, Config.SA_KEY_NAME)

# Hent moder tabellen og returner som pandas df
df = get_data_from_BQ(bq_client, Config.BRUM_PROJECT_ID, Config.DATASET_GOLD, Config.TABLE_UKE_ANTALL_GOLD_MOCK)
print(df.info())

# Grupper bare på avdelinger og ignorer innsatsgruppe
df_grouped = df.groupby(["år", "uke", "tiltaksnavn", 'avdeling'], as_index=False)["antall"].sum()

write_to_BQ(client=bq_client, table_name=Config.TABLE_UKE_ANTALL_GOLD_AVDELING_MOCK, dframe=df_grouped, 
            dataset=Config.DATASET_GOLD, disp = "WRITE_TRUNCATE", schema_name="src/data_mocking/mock_deltaker_schema.json")

