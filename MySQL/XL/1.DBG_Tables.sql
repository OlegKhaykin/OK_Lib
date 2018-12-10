DROP SEQUENCE IF EXISTS seq_dbg_process_logs;
DROP TABLE IF EXISTS dbg_log_data;
DROP TABLE IF EXISTS dbg_performance_data;
DROP TABLE IF EXISTS dbg_process_logs;

CREATE SEQUENCE seq_dbg_process_logs NOCACHE;

CREATE TABLE dbg_process_logs
(
  proc_id     SMALLINT UNSIGNED NOT NULL,
  name        VARCHAR(100) NOT NULL,
  comment_txt VARCHAR(1000),
  start_time  DATETIME(6) DEFAULT CURRENT_TIMESTAMP NOT NULL,
  end_time    DATETIME(6) NULL,
  result      VARCHAR(2048),
  CONSTRAINT pk_dbg_process_logs PRIMARY KEY(proc_id)
) ENGINE=MyISAM;

CREATE INDEX ix_dbg_process_logs_name ON dbg_process_logs(name);
GRANT SELECT ON dbg_process_logs TO everybody;

CREATE TABLE dbg_log_data
(
  proc_id                    SMALLINT UNSIGNED NOT NULL,
  tstamp                     DATETIME(6) NOT NULL,
  log_level                  DECIMAL(2) NOT NULL,
  action                     VARCHAR(255) NOT NULL,
  comment_txt                VARCHAR(21000)
) ENGINE=MyISAM;

CREATE INDEX fki_dbg_log_data_procid ON dbg_log_data(proc_id);
GRANT SELECT ON dbg_log_data TO everybody;
 

CREATE TABLE dbg_performance_data
(
  proc_id                    SMALLINT UNSIGNED NOT NULL,
  action                     VARCHAR(128),
  cnt                        SMALLINT UNSIGNED NOT NULL,
  seconds                    DECIMAL(16,6),
  CONSTRAINT pk_perfdata PRIMARY KEY(proc_id, action)
) ENGINE=MyISAM;
 
CREATE INDEX fki_dbg_perfdata_procid ON dbg_performance_data(proc_id);
GRANT SELECT ON dbg_performance_data TO everybody;

