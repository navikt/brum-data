import pandas as pd
import numpy as np
import plotly.express as px
import pandasql as psql


# Exploring plotting and connecting the created mocked data to see if we can
# recreate the plots from Abdikarims excel spreadsheet

df_deltakere = pd.read_csv('src/data_mocking/mock_deltakere.csv')
df_gjennomforinger = pd.read_csv('src/data_mocking/mock_gjennomforinger.csv')
df_tiltakstyper = pd.read_csv('src/data_mocking/mock_tiltakstyper.csv')

gjennomforinger_ungdomsavd = df_gjennomforinger.query("avdeling == 'Ungdomsavd.'")

query = """
SELECT * 
FROM df_gjennomforinger 
JOIN df_tiltakstyper 
ON df_gjennomforinger.tiltakstype_id = df_tiltakstyper.id
"""

result = psql.sqldf(query, locals())
print(result.head())