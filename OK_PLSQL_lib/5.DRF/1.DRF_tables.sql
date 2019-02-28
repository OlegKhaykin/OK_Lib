exec dbm.drop_tables('CNF_DATA_FLOW_STEPS,LOG_DRF_ERRORS,CNF_DATA_FLOWS');

CREATE TABLE cnf_data_flows
(
  data_flow_cd    VARCHAR2(20) CONSTRAINT pk_cnf_data_flows PRIMARY KEY,
  description     VARCHAR2(256),
  max_num_of_jobs NUMBER(3),
  heartbeat_dt    DATE DEFAULT SYSDATE NOT NULL,
  last_proc_id    NUMBER(20),
  signal          VARCHAR2(5) DEFAULT 'STOP' NOT NULL
   CONSTRAINT chk_cnf_data_flow_signal CHECK(signal IN ('START','STOP')) 
);

CREATE TABLE cnf_data_flow_steps
(
  data_flow_cd  VARCHAR2(20),
  set_num       NUMBER(2) DEFAULT 1 NOT NULL,
  num           NUMBER(3),
  operation     VARCHAR2(30),
   CONSTRAINT chk_cnf_data_flow_steps_oper
   CHECK(operation IN ('INSERT','UPDATE','MERGE','REPLACE','DELETE','PROCEDURE','WAIT','EXCHANGE PARTITION')),
  tgt           VARCHAR2(61) NOT NULL,
  src           VARCHAR2(2000) NOT NULL,
  whr           VARCHAR2(500),
  hint          VARCHAR2(500),
  match_cols    VARCHAR2(256),
  check_changed VARCHAR2(500),
  generate      VARCHAR2(500),
  del           VARCHAR2(500),
  versions      VARCHAR2(500),
  err_table     VARCHAR2(61),
  commit_at     NUMBER(10) DEFAULT 0 NOT NULL,
  as_job        CHAR(1) DEFAULT 'N' NOT NULL CONSTRAINT chk_cnf_data_flow_steps_as_job CHECK (as_job IN ('Y','N')),
  CONSTRAINT pk_cnf_data_flow_steps PRIMARY KEY(data_flow_cd, set_num, num),
  CONSTRAINT fk_cnf_data_flow_steps_cd FOREIGN KEY(data_flow_cd) REFERENCES cnf_data_flows ON DELETE CASCADE  
);

CREATE TABLE log_drf_errors
(
  data_flow_cd  VARCHAR2(20) NOT NULL, 
  proc_id       NUMBER(30) NOT NULL,
  err_table     VARCHAR2(30) NOT NULL,
  err_message   VARCHAR2(2000) NOT NULL,
  err_count     NUMBER(10) NOT NULL,
  CONSTRAINT pk_log_drf_errors PRIMARY KEY(data_flow_cd, proc_id, err_table, err_message)
)
ORGANIZATION INDEX;
