prompt Creating package PKG_DATA_REFRESH

CREATE OR REPLACE PACKAGE pkg_data_refresh AS
/*
 =============================================================================
 This package provides a metadata-driven ETL framework.
 
 It was developed by Oleg Khaykin. 1-201-625-3161. OlegKhaykin@gmail.com. 
 Your are allowed to use and change it as you wish, as long as you
 retain here the reference to the original developer - i.e. Oleg Khaykin.
 
 In other words, you are not allowed to remove this comment!
 =============================================================================
 
 Change history:
 -----------------------------------------------------------------------------
 30-Mar-2018, OK: new version
*/
  -- Procedure to start the Data Flow
  PROCEDURE start_data_flow(p_data_flow_cd IN VARCHAR);

  -- Procedure to stop the Data Flow
  PROCEDURE stop_data_flow(p_data_flow_cd IN VARCHAR);
  
  -- Procedure HEARTBEAT updates HEARTBEAT_DT in CNF_DATA_FLOWS
  PROCEDURE heartbeat(p_data_flow_cd IN VARCHAR2); 
  
  -- Procedure EXEC_TASK is started by via DBMS_SCHEDULER to execute one Data Flow step   
  PROCEDURE exec_task
  (
    p_job_name      IN VARCHAR2,
    p_task          IN VARCHAR2
  );
  
  -- Procedure GATHER_ERROR_STATS collect counts of errors that happened in the given Data Flow
  PROCEDURE gather_error_stats
  (
    p_data_flow_cd  IN VARCHAR2,
    p_proc_id       IN NUMBER DEFAULT NULL -- if not specified then the latest run of the Data Flow is used
  );
END;
/

CREATE OR REPLACE SYNONYM drf FOR pkg_data_refresh
/

CREATE OR REPLACE PUBLIC SYNONYM drf FOR pkg_data_refresh
/
