BEGIN
  FOR r IN
  (
    SELECT d.object_name
    FROM
    (
      SELECT 'SEQ_DBG_XLOGGER' object_name FROM dual UNION ALL
      SELECT 'DBG_PROCESS_LOGS' object_name FROM dual UNION ALL
      SELECT 'DBG_LOG_DATA' object_name FROM dual UNION ALL
      SELECT 'DBG_PERFORMANCE_DATA' object_name FROM dual UNION ALL
      SELECT 'DBG_SUPPLEMENTAL_DATA' object_name FROM dual UNION ALL
      SELECT 'DBG_SETTINGS' FROM dual
    ) d
    LEFT JOIN dba_objects dbo
      ON dbo.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
     AND dbo.object_name = d.object_name
    WHERE dbo.object_name IS NULL
  )
  LOOP
    CASE r.object_name
      WHEN 'SEQ_DBG_XLOGGER' THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE seq_dbg_xlogger INCREMENT BY 1 NOCACHE';

      WHEN 'DBG_PROCESS_LOGS' THEN
        EXECUTE IMMEDIATE '
CREATE TABLE dbg_process_logs
(
  proc_id     NUMBER(30) NOT NULL,
  name        VARCHAR2(100) NOT NULL,
  comment_txt VARCHAR2(1000),
  start_time  TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  end_time    TIMESTAMP(6),
  result      VARCHAR2(2048),
  CONSTRAINT pk_dbg_process_logs PRIMARY KEY(proc_id) USING INDEX LOCAL
)
PARTITION BY RANGE (proc_id) INTERVAL (1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
)';
        EXECUTE IMMEDIATE 'CREATE INDEX ix_dbg_process_logs_name ON dbg_process_logs(name) LOCAL';
        EXECUTE IMMEDIATE 'GRANT SELECT ON dbg_process_logs TO PUBLIC';
 
      WHEN 'DBG_LOG_DATA' THEN
        EXECUTE IMMEDIATE '
CREATE TABLE dbg_log_data
(
  proc_id                    NUMBER(30) NOT NULL,
  tstamp                     TIMESTAMP(6) NOT NULL,
  log_depth                  NUMBER(2) NOT NULL,
  pls_unit                   VARCHAR2(128) DEFAULT ''NA'' NOT NULL,
  action                     VARCHAR2(255) NOT NULL,
  comment_txt                CLOB,
  CONSTRAINT fk_logdata_proc FOREIGN KEY (proc_id) REFERENCES dbg_process_logs(proc_id) ON DELETE CASCADE
)
PARTITION BY RANGE(proc_id) INTERVAL(1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
)';
        EXECUTE IMMEDIATE 'CREATE INDEX fki_dbg_log_data_procid ON dbg_log_data(proc_id) LOCAL';
        EXECUTE IMMEDIATE 'GRANT SELECT ON dbg_log_data TO PUBLIC';

      WHEN 'DBG_PERFORMANCE_DATA' THEN
        EXECUTE IMMEDIATE '
CREATE TABLE dbg_performance_data
(
  proc_id                    NUMBER(30),
  action                     VARCHAR2(255),
  cnt                        NUMBER(10),
  seconds                    NUMBER,
  CONSTRAINT pk_perfdata PRIMARY KEY(proc_id, action) USING INDEX LOCAL, 
  CONSTRAINT fk_perfdata_proc FOREIGN KEY (proc_id) REFERENCES dbg_process_logs ON DELETE CASCADE
)
PARTITION BY RANGE(proc_id) INTERVAL(1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
)';
        EXECUTE IMMEDIATE 'GRANT SELECT ON dbg_performance_data TO PUBLIC';
    
      WHEN 'DBG_SUPPLEMENTAL_DATA' THEN
        EXECUTE IMMEDIATE '
CREATE TABLE dbg_supplemental_data
(
  proc_id   NUMBER(30)    NOT NULL,
  name      VARCHAR2(128) NOT NULL,
  tstamp    TIMESTAMP(6)  NOT NULL,
  value     SYS.ANYDATA   NOT NULL,
  CONSTRAINT dbg_suppldat_fk_proc FOREIGN KEY(proc_id) REFERENCES dbg_process_logs ON DELETE CASCADE 
)
PARTITION BY RANGE (proc_id) INTERVAL (1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
)';
        EXECUTE IMMEDIATE 'CREATE INDEX idx_dbg_suppldat_fk_proc ON dbg_supplemental_data(proc_id) LOCAL';
        EXECUTE IMMEDIATE 'GRANT SELECT ON dbg_supplemental_data TO PUBLIC';
        
      WHEN 'DBG_SETTINGS' THEN
        EXECUTE IMMEDIATE '
CREATE TABLE dbg_settings
(
  proc_name   VARCHAR2(100) CONSTRAINT pk_dbg_settings PRIMARY KEY,
  log_level   NUMBER(4) DEFAULT 0 NOT NULL  
)';
        EXECUTE IMMEDIATE 'COMMENT ON COLUMN dbg_settings.log_level IS ''Negative number taken by absolute value designates the minimal number of seconds that the process should run from start to end to be logged in the DB tables''';
    END CASE;
  END LOOP;
  
  FOR r IN
  (
    SELECT d.table_name, d.column_name, d.data_type
    FROM
    (
      SELECT 'DBG_LOG_DATA' table_name, 'PLS_UNIT' column_name, 'VARCHAR2(128) DEFAULT ''NA''' data_type
      FROM dual
    ) d
    LEFT JOIN dba_tab_columns tc
      ON tc.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
     AND tc.table_name = d.table_name
     AND tc.column_name = d.column_name
    WHERE tc.owner IS NULL
  )
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||r.table_name||' ADD '||r.column_name||' '||r.data_type;
  END LOOP;
END;
/
