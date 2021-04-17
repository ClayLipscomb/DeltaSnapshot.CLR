DROP TABLE DLTA.dlta_run CASCADE CONSTRAINTS;

CREATE TABLE DLTA.dlta_run (
	run_id								INTEGER	PRIMARY KEY,
	data_set_id							INTEGER NOT NULL,
	run_mode							VARCHAR2(20) NOT NULL,
	status_code							VARCHAR2(10) NOT NULL,
	status_message						VARCHAR2(4000),
	data_set_count						INTEGER,
	delta_count							INTEGER,
	start_date							TIMESTAMP,
	end_date							TIMESTAMP
);

--CREATE UNIQUE INDEX DLTA.dlta_run_pk ON DLTA.dlta_run(run_id);
--ALTER TABLE DLTA.dlta_run ADD ( CONSTRAINT dlta_run_pk PRIMARY KEY (run_id) USING INDEX DLTA.dlta_run_pk);