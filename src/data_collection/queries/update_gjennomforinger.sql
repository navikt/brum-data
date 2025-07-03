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