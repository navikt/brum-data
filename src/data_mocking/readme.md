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

**Data basert på Team Komet: `amt_tiltak_datastream_amttiltak_deltaker_siste_status_view`**

Fra denne kilden får vi:
- bruker_id
- gjennomforing_id (for å knytte til Valp gjennomføringer)
- status (for å se om en bruker deltar nå. Status er et mer komplisert felt i virkeligheten men vi forenkler til pågår/avsluttet for mocken)
- start_dato
- slutt_dato

**Start og slutt dato genereres til å være i et spenn innenfor perioden gjennomføringen har vart, dette vil gi veldig urealistisk perioder**

## Mock data med ingen kilde

Dette genererer vi uten noe grunnlag i datamarkedsplassen
- avdeling (For å få avdelinger innad i et kontor)

## Prosess - mocking

- `gjennomforing_silver_snapshot` blir brukt som mal for å generere deltakere som har deltakelsesperioder som passer innad i en ekte gjennomførings periode.

- Vi genererer en "silver nivå" tabell kalt `deltaker_silver_mock` med all informasjonen som er mocket. **Her skipper vi steget å joine Effekt og Komet sine tabeller.**

- Denne kan joines med `gjennomforing_silver_snapshot` på "gjennomforing_id".

1. Vi henter et subset av gjennomføringene for noen kontorer (i testcasen har vi bare Bærum)
2. Vi mocker en avdeling til hver gjennomføring for å gjøre de ansvarlige for gjennomføringen (burde bekreftes om dette er riktig tenkt)
3. Så mocker vi deltakere:
- Hent en tilfeldig gjennomføring fra listen
- Gi deltakeren en unik id og en tilfeldig innsatsgruppe
- Gi deltakeren en periode for deltakelse, som er basert på perioden gjennomføringen varer. Deltakeren sin start/slutt dato skal være innenfor spennet som gjennomføringen varer.
- Om slutt_dato er senere enn snapshottets dato (2025-07-07) så setter man status til "pågår", hvis ikke settes det til "avsluttet"
4. Deltakere merges med gjennomføringer for å få en "moder" tabell med bruker_id, gjennomføring, tiltaksnavn, avdeling, kontor, innsatsbehov, status, start_dato og slutt_dato.

## Prosess - aggregering
- **Viktig å merke at mock_aggregering er designet for å aggregere bakover tid men bare innenfor "snapshot" perioden vår. En ekte prosess måtte vært en "scheduled" prosess som aggregerer hver uke og appender i en liste**
1. Vi mapper alle periode verdiene til ukene innad i hvert år.
2. Vi iterer se gjennom hver eneste uke og teller alle tilfellene av hver kombinasjon av tiltak, avdeling og innsatsgruppe. Dette vil føre til at hver uke får 240 rader, en for hver eneste kombinasjon av de tre filterne.
3. Gjør nødvendig logikk for å inkludere rader hvor man har 0 tellinger.
4. Skrives til BigQuery for å lage vårt "gold" nivå table.
5. Vi gjør også en seperat aggregering etterpå som ignorer innsatsgruppe, for å gi hele tellinger for bare avdelinger

