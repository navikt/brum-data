import pandas as pd
import numpy as np
import os
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src import Config
from src.data_collection.functions import *

# Create client based on brum-dev
bq_client = create_client(Config.BRUM_PROJECT_ID, Config.SA_KEY_NAME)

# Requests gjennomforinger_silver_snapshot and returns it as a pandas df
df_gjennomforing = get_data_from_BQ(bq_client, Config.BRUM_PROJECT_ID, Config.DATASET_SILVER, Config.TABLE_GJENNOMFORINGER_SILVER_SNAPSHOT)

# Define deltaker dataframe
df_deltaker = pd.DataFrame(columns=["bruker_id", "innsatsgruppe", "nav_kontor", "gjennomforing_id", "status", "start_dato", "slutt_dato"])

