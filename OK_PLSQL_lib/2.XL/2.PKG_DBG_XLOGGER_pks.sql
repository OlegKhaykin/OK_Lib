CREATE OR REPLACE PACKAGE pkg_dbg_xlogger AS
/*
  This package is for debugging and performance tuning
 
  History of changes (newest to oldest):
  ------------------------------------------------------------------------------
  11-Dec-2019, OK: added procedure WRITE_SUPPL_DATA;
  24-Nov-2019, OK: P_LOG_LEVEL instead of P_DEBUG;
  10-Apr-2019, OK: used CLOB as P_COMMENT data type;
  10-Nov-2015, OK: new version;
*/
  g_proc_id     dbg_process_logs.proc_id%TYPE;

  PROCEDURE open_log
  (
    p_name        IN VARCHAR2,
    p_comment     IN CLOB DEFAULT NULL, 
    p_log_level   IN PLS_INTEGER DEFAULT 0
  );
 
  FUNCTION get_current_proc_id RETURN PLS_INTEGER;
 
  PROCEDURE begin_action
  (
    p_action      IN VARCHAR2, 
    p_comment     IN CLOB DEFAULT 'Started',
    p_log_level   IN PLS_INTEGER DEFAULT 0
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