prompt Creating sequence SEQ_DBG_XLOGGER and 4 DBG_* tables
 
begin
  for r in
  (
    select object_name, object_type
    from all_objects
    where owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
    and
    (
      object_type = 'TABLE' and object_name in
      (
        'DBG_PROCESS_LOGS', 'DBG_LOG_DATA', 'DBG_PERFORMANCE_DATA'
      )
      or
      object_type = 'SEQUENCE' and object_name = 'SEQ_DBG_XLOGGER'
    )
  )
  loop
    execute immediate 'drop '||r.object_type||' '||r.object_name||
    case when r.object_type = 'TABLE' then ' cascade constraints purge' end;
  end loop;
end;
/
 
CREATE SEQUENCE seq_dbg_xlogger INCREMENT BY 1 NOCACHE;
 
CREATE TABLE dbg_process_logs
(
  proc_id     NUMBER(30) NOT NULL,
  name        VARCHAR2(100) NOT NULL,
  comment_txt VARCHAR2(1000),
  start_time  TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
  end_time    TIMESTAMP(6),
  result      VARCHAR2(2048),
  CONSTRAINT pk_dbg_process_logs PRIMARY KEY(proc_id)
)
PARTITION BY RANGE (proc_id) INTERVAL (1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
);
 
CREATE INDEX ix_dbg_process_logs_name ON dbg_process_logs(name);
 
GRANT SELECT ON dbg_process_logs TO PUBLIC;
 
CREATE TABLE dbg_log_data
(
  proc_id                    NUMBER(30) NOT NULL,
  tstamp                     TIMESTAMP(6) NOT NULL,
  log_level                  NUMBER(2) NOT NULL,
  action                     VARCHAR2(255) NOT NULL,
  comment_txt                CLOB NOT NULL,
  CONSTRAINT fk_logdata_proc FOREIGN KEY (proc_id) REFERENCES dbg_process_logs(proc_id) ON DELETE CASCADE
)
PARTITION BY RANGE(proc_id) INTERVAL(1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
);
 
CREATE INDEX fki_dbg_log_data_procid ON dbg_log_data(proc_id) LOCAL;
 
GRANT SELECT ON dbg_log_data TO PUBLIC;
 
CREATE TABLE dbg_performance_data
(
  proc_id                    NUMBER(30),
  action                     VARCHAR2(128),
  cnt                        NUMBER(10),
  seconds                    NUMBER,
  CONSTRAINT pk_perfdata PRIMARY KEY(proc_id, action), 
  CONSTRAINT fk_perfdata_proc FOREIGN KEY (proc_id) REFERENCES dbg_process_logs ON DELETE CASCADE
)
PARTITION BY RANGE(proc_id) INTERVAL(1000)
(
  PARTITION p1 VALUES LESS THAN (1000)
);
 
CREATE INDEX fki_dbg_perfdata_procid ON dbg_performance_data(proc_id) LOCAL;
 
GRANT SELECT ON dbg_performance_data TO PUBLIC;
