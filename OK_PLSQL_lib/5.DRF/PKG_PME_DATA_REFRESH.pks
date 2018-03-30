prompt Creating package PKG_PME_DATA_REFRESH

CREATE OR REPLACE PACKAGE pkg_pme_data_refresh AS
/*
 =============================================================================
 This package provides a metadata-driven ETL framework
 for automated data refresh in target DB tables - according to
 the data changes in the source tables that are accessible through
 the corresponding views.
 This is somewhat similar to Oracle materialized views fast refresh mechanism
 except this solution provides more control over the data refresh process
 and does not require MV log creation on the source tables.
  
 Controlling metadata is stored in 2 tables:
 - PME_DATA_REFRESH_JOBS;
 - PME_DATA_REFRESH_LIST;
 =============================================================================
 Change history:
 -----------------------------------------------------------------------------
 02-Feb-2013, OK: added parameter P_COALESCE to GATHER_ERROR_STATS; 
 
 11-Dec-2012, OK: added procedure PURGE_ERRORS;
 
 30-Nov-2012, OK: added procedure GATHER_ERROR_STATS;
 
 26-Nov-2012, OK: added parameter P_SLAVE_ID to EXEC_TASK procedure;
 
 18-Nov-2012, OK: added parameter P_ERROR_LOG_TABLE to SETUP procedure;
 
 22-Oct-2012, OK: added procedure EXEC_TASK;
 
 12-Sep-2012, OK:
 - added parameter P_SET_CTX_PROC to SETUP procedure;
 - added SET_CONTEXT_PROCEDURE processing to MASTER procedure;
 - added procedure HARTBEAT;
 
 25-Jul-2012, OK: added procedures START_PROCESS and STOP_PROCESS;
*/
  PROCEDURE set_min_ts(p_ts IN TIMESTAMP); -- this is mostly for testing purposes
  PROCEDURE set_slave_num(p_num IN PLS_INTEGER); -- this is mostly for testing purposes

  -- Procedure to create or change a refresh job
  PROCEDURE setup
  (
    p_refresh_type             IN VARCHAR2, 
    p_signal                   IN VARCHAR2 DEFAULT NULL, 
    p_data_change_audit_view   IN VARCHAR2 DEFAULT NULL, 
    p_job_assignment_table     IN VARCHAR2 DEFAULT NULL,
    p_error_log_table          IN VARCHAR2 DEFAULT NULL,
    p_batch_size               IN NUMBER DEFAULT NULL, 
    p_max_num_of_slaves        IN NUMBER DEFAULT NULL, 
    p_sleep_time_in_seconds    IN NUMBER DEFAULT NULL, 
    p_overlap_seconds          IN NUMBER DEFAULT NULL,
    p_before_proc              IN VARCHAR2 DEFAULT NULL,
    p_after_proc               IN VARCHAR2 DEFAULT NULL,
    p_last_processed_change_ts IN TIMESTAMP DEFAULT NULL 
  );

  FUNCTION get_slave_num RETURN PLS_INTEGER;
  FUNCTION get_min_ts RETURN TIMESTAMP;

  -- Procedure to start the refresh process
  PROCEDURE start_process(p_refresh_type IN VARCHAR);

  -- Procedure to stop the refresh process
  PROCEDURE stop_process(p_refresh_type IN VARCHAR);
  
  -- Procedure HEARTBEAT updates UPDATE_DTIME in the PME_DATA_REFRESH_JOBS
  PROCEDURE heartbeat(p_refresh_type IN VARCHAR2); 
  
  PROCEDURE master(p_refresh_type IN VARCHAR2);

  PROCEDURE slave
  (
    p_assignment_table  IN VARCHAR2,
    p_refresh_type      IN VARCHAR2,
    p_slave_number      IN PLS_INTEGER,
    p_key_column        IN VARCHAR2
  );
  
  -- This procedure is started by SLAVE using DBMS_SCHEDULER to execute one step of the refresh process  
  PROCEDURE exec_task
  (
    p_slave_id IN NUMBER,
    p_slave_number IN PLS_INTEGER,
    p_job_name IN VARCHAR2,
    p_task IN VARCHAR2
  );

  PROCEDURE gather_error_stats(p_refresh_type IN VARCHAR2, p_coalesce IN BOOLEAN DEFAULT FALSE);
  
  PROCEDURE purge_errors(p_refresh_type IN VARCHAR2, p_err_tables IN VARCHAR2 DEFAULT NULL); 
END;
/
CREATE OR REPLACE SYNONYM drf FOR pkg_pme_data_refresh;
CREATE OR REPLACE PUBLIC SYNONYM drf FOR pkg_pme_data_refresh;