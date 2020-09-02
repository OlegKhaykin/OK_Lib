CREATE OR REPLACE PACKAGE debuger.pkg_dbg_xlogger AS
/*
  This package is for debugging and performance tuning
 
  History of changes (newest to oldest):
  ------------------------------------------------------------------------------
  18-Feb-2020, OK: implemented adjustable log level setting (from another session);
  26-Dec-2019, OK: in BEGIN_ACTION, default value of P_LOG_LEVEL is 1 instead of 0;
  11-Dec-2019, OK: added procedure WRITE_SUPPL_DATA;
  24-Nov-2019, OK: P_LOG_LEVEL instead of P_DEBUG;
  10-Apr-2019, OK: used CLOB as P_COMMENT data type;
  10-Nov-2015, OK: new version;
*/
  PROCEDURE set_log_level
  (
    p_proc_name         IN VARCHAR2,
    p_session_client_id IN VARCHAR2,
    p_log_level         IN NUMBER
  );
  
  PROCEDURE reset_log_level
  (
    p_proc_name         IN VARCHAR2,
    p_session_client_id IN VARCHAR2
  );
  
  PROCEDURE open_log
  (
    p_name        IN VARCHAR2,
    p_comment     IN CLOB DEFAULT NULL, 
    p_log_level   IN PLS_INTEGER DEFAULT 0,
    p_pls_unit    IN VARCHAR2 DEFAULT NULL,
    p_identify    IN BOOLEAN DEFAULT TRUE
  );
 
  FUNCTION get_current_proc_id RETURN PLS_INTEGER;
 
  PROCEDURE begin_action
  (
    p_action      IN VARCHAR2, 
    p_comment     IN CLOB DEFAULT 'Started',
    p_log_level   IN PLS_INTEGER DEFAULT 1,
    p_pls_unit    IN VARCHAR2 DEFAULT 'NA'
  );
 
  PROCEDURE end_action(p_comment IN CLOB DEFAULT 'Completed');
  
  PROCEDURE write_suppl_data(p_name IN dbg_supplemental_data.name%TYPE, p_value IN SYS.AnyData);
 
  PROCEDURE close_log(p_result IN VARCHAR2 DEFAULT NULL, p_dump IN BOOLEAN DEFAULT FALSE);
  
  PROCEDURE spool_log(p_where IN VARCHAR2 DEFAULT NULL, p_max_rows IN PLS_INTEGER DEFAULT 100);
 
  PROCEDURE cancel_log;
 
END;
/

CREATE OR REPLACE SYNONYM xl FOR pkg_dbg_xlogger;
--CREATE OR REPLACE PUBLIC SYNONYM xl FOR pkg_dbg_xlogger;
GRANT EXECUTE ON xl TO PUBLIC;