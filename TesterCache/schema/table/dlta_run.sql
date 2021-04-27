DROP TABLE DLTA.dlta_run CASCADE CONSTRAINTS;

CREATE TABLE DLTA.dlta_run (
	run_id								INTEGER	PRIMARY KEY,
	subscription_data_set_id			INTEGER NOT NULL,
	run_mode							VARCHAR2(20) NOT NULL,
	status_code							VARCHAR2(10) NOT NULL,
	status_message						VARCHAR2(4000),
	data_set_count						INTEGER,
	delta_count							INTEGER,
	start_date							TIMESTAMP,
	end_date							TIMESTAMP
);
