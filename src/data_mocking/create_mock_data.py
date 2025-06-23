import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import sys
sys.path.insert(0, 'src')
from data_collection.functions import *

def create_random_date_span(start, end):

    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def create_mock_gjennomforinger():

    columns = ['id', 'tiltakstype_id', 'enhetsnummer', 'avdeling', 'start_dato', 'slutt_dato']
    rows = []
    start_span = datetime(2009, 1, 1)
    end_span = datetime(2027, 1, 1)
    avdelinger = ['A&H', 'KIA', 'Oppfølging og øk.', 'Ungdomsavd.', 'Veiledingsavd.']

    for i in range(1000):
        # Ensures that the start/end dates for the mock "gjennomføringer" is sequential and not random
        start_date = create_random_date_span(start_span, end_span - timedelta(days=21))
        end_date = create_random_date_span(start_date + timedelta(days=21), end_span)

        row = {
            'id': 'gjennomføring' + str(i),
            'tiltakstype_id': 'tiltak' + str(np.random.randint(1, 9)),
            'enhetsnummer': '0219',
            'avdeling': random.choice(avdelinger),
            'start_dato': start_date.date(),
            'slutt_dato': end_date.date()
        } 
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv('src/data_mocking/mock_gjennomforinger.csv', index=False)

def create_mock_deltakere():

    columns = ['id', 'gjennomforing_id', 'innsatsbehov', 'start_dato', 'slutt_dato', ]
    rows = []
    gjennomforinger = pd.read_csv('src/data_mocking/mock_gjennomforinger.csv', parse_dates=['start_dato', 'slutt_dato'], index_col=False)
    innsatsbehov = ['Landegruppe3', 'S.Bestemt', 'S.Tilpasset']
    
    for i in range (10000):

        random_gjennomforing = gjennomforinger.sample(n=1)
        
        # Create new start and end dates in the span of the relevant gjennomforing that has been extracted (wow!)
        start_span = random_gjennomforing['start_dato'].iloc[0]
        end_span = random_gjennomforing['slutt_dato'].iloc[0]
        start_date = create_random_date_span(start_span, end_span - timedelta(days=1))
        end_date = create_random_date_span(start_date + timedelta(days=1), end_span)

        row = {
            'id': 'deltaker' + str(i),
            'gjennomforing_id': random_gjennomforing['id'].iloc[0],
            'innsatsbehov': random.choice(innsatsbehov),
            'start_dato': start_date.date(),
            'slutt_dato': end_date.date()
        }
        rows.append(row)
    
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv('src/data_mocking/mock_deltakere.csv', index=False)

        
def write_mock_to_BQ():
    df_tiltakstyper = pd.read_csv('src/data_mocking/mock_tiltakstyper.csv')
    df_gjennomforinger = pd.read_csv('src/data_mocking/mock_gjennomforinger.csv', parse_dates=['start_dato', 'slutt_dato'])
    df_deltakere = pd.read_csv('src/data_mocking/mock_deltakere.csv', parse_dates=['start_dato', 'slutt_dato'])
    PROJECT_ID = 'brum-dev-b72f'
    SA_KEY_NAME = 'service-account-key'
    DATASET = 'mock_data'
    bq_client_mock_data = create_client(PROJECT_ID, SA_KEY_NAME)

    # Write mock gjenomforinger
    write_to_BQ(client=bq_client_mock_data, table_name="mock_gjennomforinger", dframe=df_gjennomforinger, dataset=DATASET, disp = "WRITE_TRUNCATE", schema_name='src/data_mocking/mock_schema.json')
    write_to_BQ(client=bq_client_mock_data, table_name="mock_tiltakstyper", dframe=df_tiltakstyper, dataset=DATASET, disp = "WRITE_TRUNCATE", schema_name='src/data_mocking/mock_schema.json')
    write_to_BQ(client=bq_client_mock_data, table_name="mock_deltakere", dframe=df_deltakere, dataset=DATASET, disp = "WRITE_TRUNCATE", schema_name='src/data_mocking/mock_schema.json')

    
#create_mock_gjennomforinger()
#create_mock_deltakere()
write_mock_to_BQ()