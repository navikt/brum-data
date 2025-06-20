import pandas as pd
import numpy as np
import plotly.express as px
import pandasql as psql


# Exploring plotting and connecting the created mocked data to see if we can
# recreate the plots from Abdikarims excel spreadsheet

df_deltakere = pd.read_csv('src/data_mocking/mock_deltakere.csv')
df_gjennomforinger = pd.read_csv('src/data_mocking/mock_gjennomforinger.csv')
df_tiltakstyper = pd.read_csv('src/data_mocking/mock_tiltakstyper.csv')

# Only get one "avdeling"
gjennomforinger_ungdomsavd = df_gjennomforinger.query("avdeling == 'Ungdomsavd.'")

# Join with tiltakstyper to get the name of the "tiltak"
query = """
SELECT
    a.id,
    a.tiltakstype_id,
    a.enhetsnummer,
    a.avdeling,
    a.start_dato,
    a.slutt_dato,
    b.navn,
    b.tiltakskode,
    b.arena_tiltakskode
FROM gjennomforinger_ungdomsavd AS a
JOIN df_tiltakstyper AS b
ON a.tiltakstype_id = b.id
"""
# Apply the query
result = psql.sqldf(query, locals())
print(result)

# Extremely simple plot showing amount of "gjennomføringer" of each "tiltak" in the "Ungdomsavd." avdeling 
fig = px.histogram(result, x=result['navn'], nbins=10)
fig.show()