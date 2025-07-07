# Data mocking forklaring

## Ekte data
#### Vi baserer oss på at vi har dataen om gjennomføringer fra den faktisk valp kilden. 

I tiltak_silver har vi "gjennomforinger_silver" som dynamisk blir oppdatert hver dag.
For å ikke gjøre unødvendige utregninger så mocker vi ikke data til å passe den dynamisk oppdaterte dataen.

Vi tar derfor et snapshot av dataen (07.07.2025) og mocker basert på hva dataen var den dagen, tabellen heter gjennomforinger_silver_snapshot.

## Mock data basert på ekte data
**Data basert på Team Effekt: `vent_person_periode_v`**

Fra denne kilden får vi:
- bruker_id (heter egentlig deltaker_id men vi hopper over renaming for å letter knytte med Komet som har kalt det samme feltet bruker_id)
- innsatsgruppekode (de forskjellige innsatsbehovene)
- nav_kontor

**Data basert på Team Komet: `amt_tiltak_datastream_amttiltak_deltaker_siste_status_view`**

Fra denne kilden får vi:
- bruker_id
- gjennomforing_id (for å knytte til Valp gjennomføringer)
- start_dato
- slutt_dato

**Start og slutt dato genereres til å være i et spenn innenfor perioden gjennomføringen har vart, dette vil gi veldig urealistisk perioder**

## Mock data med ingen kilde

Dette genererer vi uten noe grunnlag i datamarkedsplassen
- avdeling_navn (For å få avdelinger innad i et kontor)

