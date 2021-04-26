DROP TABLE DLTA.dlta_cache_snapshot CASCADE CONSTRAINTS;

CREATE TABLE DLTA.dlta_cache_snapshot (
	subscription_data_set_id			INTEGER NOT NULL,
    run_id                              INTEGER NOT NULL,
    entity_identifier                   VARCHAR2(4000) NOT NULL,
    entity_delta_code                   VARCHAR2(3) NOT NULL,
    entity_delta_date                   TIMESTAMP NOT NULL,
    entity_data_current                 CLOB NOT NULL,
    entity_data_previous                CLOB
);

ALTER TABLE DLTA.dlta_cache_snapshot
ADD CONSTRAINT fk_cache_entry_run
  FOREIGN KEY (run_id)
  REFERENCES dlta_run (run_id);