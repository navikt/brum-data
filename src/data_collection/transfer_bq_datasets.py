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

from functions import create_client, write_to_BQ, get_data_from_BQ

# Brum project
PROJECT_ID = 'brum-dev-b72f'
SA_KEY_NAME = 'service-account-key'

# Create client based on brum-dev
bq_client = create_client(PROJECT_ID, SA_KEY_NAME)

# Requests the data from BiqQuery and returns a dataframe
#df_tiltakstyper = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'tiltakstype_view')
df_gjennomforing = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_view')
df_gjennomforing_enhet = get_data_from_BQ(bq_client, 'team-mulighetsrommet-prod-5492', 'mulighetsrommet_api_datastream', 'gjennomforing_nav_enhet_view')

# Queries to update the tables for gjennomforing and nav_enhet_gjennomforing with any changed or new rows
update_gjennomforing_query = """
MERGE `tiltak_bronze.gjennomforinger_bronze` AS target
USING `team-mulighetsrommet-prod-5492.mulighetsrommet_api_datastream.gjennomforing_view` AS source
ON target.id = source.id
WHEN MATCHED AND (
     target.oppdatert_tidspunkt IS DISTINCT FROM source.oppdatert_tidspunkt
) THEN
  UPDATE SET
    id = source.id,
    tiltakstype_id = source.tiltakstype_id,
    avtale_id = source.avtale_id,
    tiltaksnummer = source.tiltaksnummer,
    start_dato = source.start_dato,
    slutt_dato = source.slutt_dato,
    opprettet_tidspunkt = source.opprettet_tidspunkt,
    oppdatert_tidspunkt = source.oppdatert_tidspunkt,
    avsluttet_tidspunkt = source.avsluttet_tidspunkt
WHEN NOT MATCHED THEN
  INSERT (id, tiltakstype_id, avtale_id, tiltaksnummer, start_dato, slutt_dato, 
  opprettet_tidspunkt, oppdatert_tidspunkt, avsluttet_tidspunkt)
  VALUES (source.id, source.tiltakstype_id, source.avtale_id, source.tiltaksnummer, source.start_dato, source.slutt_dato, 
  source.opprettet_tidspunkt, source.oppdatert_tidspunkt, source.avsluttet_tidspunkt)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
"""

update_gjennomforing_enhet_query = """
MERGE `tiltak_bronze.nav_enhet_bronze` AS target
USING `team-mulighetsrommet-prod-5492.mulighetsrommet_api_datastream.gjennomforing_nav_enhet_view` AS source
ON target.gjennomforing_id = source.gjennomforing_id
WHEN NOT MATCHED THEN 
  INSERT (gjennomforing_id, enhetsnummer)
  VALUES (source.gjennomforing_id, source.enhetsnummer)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
"""

def get_raw_data():
    logging.info("Attempting to get get 'gjennomforing_view' from BQ")
    try:
        bq_client.query(update_gjennomforing_query)
        logging.info("Query handled successfully")
    except Exception as e:
        logging.error(f"Query failed: {e}")

    logging.info("Attempting to get get 'gjennomforing_nav_enhet_view' from BQ")
    try:
        bq_client.query(update_gjennomforing_enhet_query)
        logging.info("Query handled successfully")
    except Exception as e:
        logging.error(f"Query failed: {e}")

get_raw_data()