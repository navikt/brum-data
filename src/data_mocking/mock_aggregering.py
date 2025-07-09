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
df = get_data_from_BQ(bq_client, Config.BRUM_PROJECT_ID, Config.DATASET_SILVER, Config.TABLE_DELTAKER_MERGED_SILVER_MOCK)

# Konverter til datetime
df['start_dato'] = pd.to_datetime(df['start_dato'])
df['slutt_dato'] = pd.to_datetime(df['slutt_dato'])

# Lag datopunkter
weeks = pd.date_range(start='2022-01-03', end='2025-07-07', freq='W-MON')
week_df = pd.DataFrame({'week_start': weeks})
week_df['year'] = week_df['week_start'].dt.isocalendar().year
week_df['week'] = week_df['week_start'].dt.isocalendar().week
week_df['week_end'] = week_df['week_start'] + timedelta(days=6)

# Henter alle unike tilfeller av de 3 "filterene" (CHATGPT)
# Trengs for å garantere at man får en kombinasjon av hvert filter for hver uke
tiltak_values = df['tiltaksnavn'].dropna().unique()
gruppe_values = df['innsatsgruppe'].dropna().unique()
avdeling_values = df['avdeling'].dropna().unique()

# Regner ut "kartesisk produkt" av alle mulige kombinasjoner av de 3 filterene (CHATGPT)
group_combinations = pd.DataFrame(
    list(product(tiltak_values, gruppe_values, avdeling_values)),
    columns=['tiltaksnavn', 'innsatsgruppe', 'avdeling']
)

# 
week_df['_tmp'] = 1
group_combinations['_tmp'] = 1
full_grid = pd.merge(week_df, group_combinations, on='_tmp').drop(columns=['_tmp'])

# Actual counts per week/group combo
results = []

for i, week_row in week_df.iterrows():
    week_start = week_row['week_start']
    week_end = week_row['week_end']
    year = week_row['year']
    week = week_row['week']
    
    # Filter active rows
    active = df[(df['start_dato'] <= week_end) & (df['slutt_dato'] >= week_start)]

    if not active.empty:
        grouped = (
            active
            .groupby(['tiltaksnavn', 'innsatsgruppe', 'avdeling'])
            .size()
            .reset_index(name='count')
        )
        grouped['year'] = year
        grouped['week'] = week
        results.append(grouped)

# Combine all weekly results
count_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame(columns=['tiltaksnavn', 'innsatsgruppe', 'avdeling', 'antall', 'year', 'week'])

# Merge counts into full grid (left join)
final_df = pd.merge(
    full_grid,
    count_df,
    on=['year', 'week', 'tiltaksnavn', 'innsatsgruppe', 'avdeling'],
    how='left'
)

# Fill missing counts with 0
final_df['count'] = final_df['count'].fillna(0).astype(int)

# Optional: sort and drop extra columns
final_df = final_df.sort_values(['year', 'week', 'tiltaksnavn'])

final_df = final_df.rename(columns={"year":"år", "week":"uke", "count":"antall"})
final_df = final_df.drop(columns={"week_start", "week_end"})

print(final_df.head())

write_to_BQ(client=bq_client, table_name=Config.TABLE_UKE_ANTALL_GOLD_MOCK, dframe=final_df, 
            dataset=Config.DATASET_GOLD, disp = "WRITE_TRUNCATE", schema_name="src/data_mocking/mock_deltaker_schema.json")



