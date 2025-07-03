"""
Requests the different tables and views from other projects and transfers them to brum-dev-b72f.tiltak_bronze
This is the pure raw data extracted without any processing
Tiltakstyper is a static table so we do not create it again, if this changes several steps must be updated
"""
import pandas as pd
import numpy as np
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from pathlib import Path
from functions import *

# Brum project
PROJECT_ID = 'brum-dev-b72f'
SA_KEY_NAME = 'service-account-key'

# Create client based on brum-dev
bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

# Requests the data from BiqQuery and returns a dataframe
#df_tiltakstyper = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'tiltakstype_view')
df_gjennomforing = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_view')
df_gjennomforing_enhet = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_nav_enhet_view')
#df_deltakere = get_data_from_BQ(bq_client, 'FYLL INN')

# Get queries to read source data and update raw data in tiltak_bronze
base_path = Path(__file__).parent
update_gjennomforinger_query = Path(base_path/"queries/update_gjennomforinger.sql").read_text(encoding="utf-8")
update_nav_enhet_query = Path(base_path/"queries/update_nav_enhet.sql").read_text(encoding="utf-8")

# Run queries to update the raw data
update_raw_data(bq_client, update_gjennomforinger_query)
update_raw_data(bq_client, update_nav_enhet_query)
