import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def create_random_date_span(start, end):

    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def create_mock_gjennomforinger():

    columns = ['id', 'tiltakstype_id', 'start_dato', 'slutt_dato']
    rows = []
    start_span = datetime(2009, 1, 1)
    end_span = datetime(2027, 1, 1)

    for i in range(200):
        # Ensures that the start/end dates for the mock "gjennomføringer" is sequential and not random
        start_date = create_random_date_span(start_span, end_span - timedelta(days=21))
        end_date = create_random_date_span(start_date + timedelta(days=21), end_span)

        row = {
            'id': 'gjennomføring' + str(i),
            'tiltakstype_id': np.random.randint(1, 9),
            'start_dato': start_date.date(),
            'slutt_dato': end_date.date()
        } 
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv('src/data_mocking/mock_gjennomforinger.csv', index=False)

def create_mock_deltakere():

    columns = ['id', 'gjennomforing_id', 'start_dato', 'slutt_dato', ]
    rows = []
    gjennomforinger = pd.read_csv('src/data_mocking/mock_gjennomforinger.csv', parse_dates=['start_dato', 'slutt_dato'], index_col=False)
    
    for i in range (5000):

        random_gjennomforing = gjennomforinger.sample(n=1)
        
        # Create new start and end dates in the span of the relevant gjennomforing that has been extracted (wow!)
        start_span = random_gjennomforing['start_dato'].iloc[0]
        end_span = random_gjennomforing['slutt_dato'].iloc[0]
        start_date = create_random_date_span(start_span, end_span - timedelta(days=1))
        end_date = create_random_date_span(start_date + timedelta(days=1), end_span)

        row = {
            'id': 'deltaker' + str(i),
            'gjennomforing_id': random_gjennomforing['id'].iloc[0],
            'start_dato': start_date.date(),
            'slutt_dato': end_date.date()
        }
        rows.append(row)
    
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv('src/data_mocking/mock_deltakere.csv', index=False)

        

create_mock_gjennomforinger()
create_mock_deltakere()