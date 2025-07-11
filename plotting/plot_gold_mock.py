import pandas as pd
import numpy as np
import plotly.express as px
import pandasql as psql
import os
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src import Config
from src.data_collection.functions import *

# Lag en BigQuery klient med brum service account
bq_client = create_client(Config.BRUM_PROJECT_ID, Config.SA_KEY_NAME)

df = get_data_from_BQ(bq_client, Config.BRUM_PROJECT_ID, Config.DATASET_GOLD, Config.TABLE_UKE_ANTALL_GOLD_MOCK)

df_uke = df[(df["år"] == 2025) & (df["uke"] == 10)]

print(df_uke.info())

df_grouped = df_uke.groupby('tiltaksnavn', as_index=False)['antall'].sum()

fig = px.bar(df_uke,
             x='tiltaksnavn',
             y='antall',
             title='Antall per Tiltaksnavn',
             labels={'tiltaksnavn': 'Tiltaksnavn', 'antall': 'Antall'},
             text='antall')

fig.update_layout(xaxis_tickangle=-45)

fig.show()