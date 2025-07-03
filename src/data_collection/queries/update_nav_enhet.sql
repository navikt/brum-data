MERGE `tiltak_bronze.nav_enhet_bronze` AS target
USING `team-mulighetsrommet-prod-5492.mulighetsrommet_api_datastream.gjennomforing_nav_enhet_view` AS source
ON target.gjennomforing_id = source.gjennomforing_id
WHEN NOT MATCHED THEN 
  INSERT (gjennomforing_id, enhetsnummer)
  VALUES (source.gjennomforing_id, source.enhetsnummer)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE