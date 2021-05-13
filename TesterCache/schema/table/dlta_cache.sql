DROP TABLE DLTA.dlta_cache_snapshot CASCADE CONSTRAINTS;

CREATE TABLE DLTA.dlta_cache_snapshot (
    cache_snapshot_id                   INTEGER PRIMARY KEY,
	subscription_data_set_id			INTEGER NOT NULL,
    run_id                              INTEGER NOT NULL,
    entity_identifier                   VARCHAR2(4000) NOT NULL,
    entity_delta_code                   VARCHAR2(3) NOT NULL,
    entity_delta_date                   TIMESTAMP WITH TIME ZONE NOT NULL,
    entity_data_current                 CLOB NOT NULL,
    entity_data_previous                CLOB
);

ALTER TABLE DLTA.dlta_cache_snapshot
ADD CONSTRAINT fk_cache_entry_run
  FOREIGN KEY (run_id)
  REFERENCES dlta_run (run_id);

CREATE UNIQUE INDEX DLTA.dlta_cache_run_set_entity ON DLTA.dlta_cache_snapshot(run_id, subscription_data_set_id, entity_identifier);