import pandas as pd
import numpy as np
import os
import sys 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src import Config
from src.data_collection.functions import *

# Lag en BigQuery klient med brum service account
bq_client = create_client(Config.BRUM_PROJECT_ID, Config.SA_KEY_NAME)

# Hent gjennomføringer fra BQ og returner som en pandas df
df_gjennomforing = get_data_from_BQ(bq_client, Config.BRUM_PROJECT_ID, Config.DATASET_SILVER, Config.TABLE_GJENNOMFORINGER_SILVER_SNAPSHOT)
# Fjern gjennomføringer med slutt_dato = null
df_gjennomforing = df_gjennomforing.dropna(subset=["slutt_dato"])
# Filtrer dataen til å bare inkludere gjennomføringer fra bestemte kontor (Bærum = 0219)
df_gjennomforing = df_gjennomforing[df_gjennomforing["enhetsnummer"] == "0219"]
# Gi hvert kontor forskjellige lister med avdelinger for å gjøre dataen mer realistisk (og for å lage dynamiske kontor-lister i frontend)
avdelinger_0219 = ["A&H", "KIA", "Oppfølging og Øk.", "Ungdomsavd.", "Veiledningsavd."]
avdelinger_0000 = ["A&H", "KIA", "Ungdomsavd.", "Veiledningsavd."] #TODO
avdelinger_0001 = ["A&H", "Ungdomsavd.", "Veiledningsavd."] #TODO
df_gjennomforing["avdeling"] = np.random.choice(avdelinger_0219, size=len(df_gjennomforing))

# Liste av innsatsgrupper en deltaker kan være medlem av
innsatsgrupper = ["IKVAL", "IVURD", "VARIG", "BATT", "BFORM", "UKJENT"]

# Kode for å generere én deltaker
def generer_deltaker(gjennomforing, deltakerNr):
    # Basert på den tilfeldig valgte gjennomføringen fra listen generer vi en deltaker
    bruker_id = deltakerNr
    deltaker_innsatsgruppe = np.random.choice(innsatsgrupper, size=1)[0]
    deltaker_gjennomforing = gjennomforing["gjennomforing_id"]

    # VANSKELIGE BITEN
    # Generer et datospenn for deltakelse som er innenfor spennet gjennomføringen varer
    start_gjennomforing = pd.to_datetime(gjennomforing["start_dato"])
    slutt_gjennomforing = pd.to_datetime(gjennomforing["slutt_dato"])

    total_dager = (slutt_gjennomforing - start_gjennomforing).days

    if total_dager < 7:
        # fallback: use entire span
        deltaker_start_dato = start_gjennomforing
        deltaker_slutt_dato = slutt_gjennomforing
    else:
        max_start_offset = total_dager - 7
        random_start_offset = np.random.randint(0, max_start_offset + 1)
        deltaker_start_dato = start_gjennomforing + pd.Timedelta(days=random_start_offset)

        # slutt_dato er minst 7 dager etter start dato
        min_end_offset = 7
        max_end_offset = total_dager - random_start_offset
        random_end_offset = np.random.randint(min_end_offset, max_end_offset + 1)
        deltaker_slutt_dato = deltaker_start_dato + pd.Timedelta(days=random_end_offset)

    # Sett status til pågår hvis deltakeren er aktiv (slutt dato er senere en "dagens dato")
    grense_dato = pd.to_datetime("2025-07-07") # Basert på når vi tok data snapshot (dagens dato)
    status = "pågår" if deltaker_slutt_dato > grense_dato else "avsluttet"

    return {
        'bruker_id': bruker_id,
        'innsatsgruppe': deltaker_innsatsgruppe,
        'gjennomforing_id': deltaker_gjennomforing,
        'start_dato': deltaker_start_dato,
        'slutt_dato': deltaker_slutt_dato,
        'status': status
    }

# Lag deltakere basert på gjennomforing_id og de tilsvarende datoene
deltakere = []
for i in range(0, 1000):
    random_gjennomforing = df_gjennomforing.sample(n=1)
    deltaker = generer_deltaker(random_gjennomforing.iloc[0], "deltaker"+str(i))
    deltakere.append(deltaker)

df_deltaker = pd.DataFrame(deltakere, columns=["bruker_id", "innsatsgruppe", "gjennomforing_id", "start_dato", "slutt_dato", "status"])
    

write_to_BQ(client=bq_client, table_name=Config.TABLE_DELTAKER_SILVER_MOCK, dframe=df_deltaker, 
            dataset=Config.DATASET_SILVER, disp = "WRITE_TRUNCATE", schema_name="src/data_mocking/mock_deltaker_schema.json")

df_gjennomforing_for_merge = df_gjennomforing.drop(columns=["start_dato", "slutt_dato", "opprettet_tidspunkt",
                                                            "oppdatert_tidspunkt", "avsluttet_tidspunkt"])

# Merge gjennomføringer med deltakere for å lage en "moder" tabell med all data
merged_df = df_deltaker.merge(df_gjennomforing_for_merge, on="gjennomforing_id", how="inner")
# Rename kolonne og stokk om for lesbarhet
merged_df = merged_df.rename(columns={"navn":"tiltaksnavn"})
merged_df = merged_df[["bruker_id", "innsatsgruppe", "enhetsnummer", "tiltaksnavn", "gjennomforing_id", "avdeling", "start_dato", "slutt_dato", "status"]]

write_to_BQ(client=bq_client, table_name=Config.TABLE_DELTAKER_MERGED_SILVER_MOCK, dframe=merged_df, 
            dataset=Config.DATASET_SILVER, disp = "WRITE_TRUNCATE", schema_name="src/data_mocking/mock_deltaker_schema.json")