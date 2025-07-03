"""
Script to join and do prelimenary cleaning of the raw data,
removing double id's and replacing id's with names where possible
"""

import pandas as pd
import numpy as np
import os

from functions import *

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Brum project
PROJECT_ID = 'brum-dev-b72f'
DATASET_BRONZE = 'tiltak_bronze'
DATASET_SILVER = 'tiltak_silver'
SA_KEY_NAME = 'service-account-key'

# Create client based on brum-dev
bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

df_gjennomforing_enhet = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'nav_enhet_bronze')

# Get gjennomføringer and reformat all date columns to the same format
df_gjennomforinger = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'gjennomforinger_bronze')
date_columns = ["start_dato", "slutt_dato", "opprettet_tidspunkt", "oppdatert_tidspunkt", "avsluttet_tidspunkt"]
for col in date_columns:
    df_gjennomforinger[col] = pd.to_datetime(df_gjennomforinger[col], errors="coerce")
# Remove rows with gjennomføringer from before 2022 (out of scope), also keep null "slutt_dato" as that means they are ongoing
df_gjennomforinger = df_gjennomforinger[(df_gjennomforinger["slutt_dato"] >= "2022-01-01") | (df_gjennomforinger["slutt_dato"].isna())]
# Remove columns: tiltaksnummer and avtale_id as they are remnants from the "arena" migration
df_gjennomforinger.drop(columns=["tiltaksnummer", "avtale_id"], inplace=True)

# Get tiltakstyper to make sure we only have the relevant "gjennomføringer" and to get their names instead of Id's
df_tiltakstyper = get_data_from_BQ(bq_client, PROJECT_ID, DATASET_BRONZE, 'tiltakstyper_bronze')
df_tiltakstyper.rename(columns={"id": "tiltakstype_id"}, inplace=True)

# Merge gjennomføringer and tiltakstyper to get relevant gjennomføringer and get the name of "tiltakstype"
df_gjennomforiner_merged = pd.merge(df_gjennomforinger, df_tiltakstyper, how="inner", on="tiltakstype_id")
# Drop extra columns, rename the gjennomførings id and reorder to be more readable
df_gjennomforiner_merged.drop(columns=["tiltakskode", "arena_tiltakskode", "tiltakstype_id"], inplace=True)
df_gjennomforiner_merged.rename(columns={"id": "gjennomforing_id"}, inplace=True)
df_gjennomforiner_merged = df_gjennomforiner_merged[["gjennomforing_id", "navn", "start_dato", "slutt_dato",
                                                     "opprettet_tidspunkt", "oppdatert_tidspunkt", "avsluttet_tidspunkt"]]

write_to_BQ(bq_client, table_name="gjennomforinger_silver", dframe=df_gjennomforiner_merged, dataset=DATASET_SILVER, disp = "WRITE_TRUNCATE")
