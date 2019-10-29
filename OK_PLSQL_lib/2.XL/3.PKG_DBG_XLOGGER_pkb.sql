CREATE OR REPLACE PACKAGE BODY pkg_dbg_xlogger AS
/*
  This package is for debugging and performance tuning
 
  History of changes - newest to oldest:
  -----------------------------------------------------------------------------
  29-Oct-2019, OK: TYPE stats_colection - INDEX BY VARCHAR(255);
  26-Nov-2019, OK: bug fix in SPOOL_LOG;
  10-Apr-2019, OK: used CLOB as P_COMMENT data type;
  10-Nov-2015, OK: new version;
*/
  TYPE stats_record IS RECORD
  (
    tstamp      TIMESTAMP WITH TIME ZONE,
    cnt         PLS_INTEGER,
    dur         INTERVAL DAY TO SECOND
  );
  TYPE stats_collection IS TABLE OF stats_record INDEX BY VARCHAR2(255);
 
  TYPE action_stack_record IS RECORD
  (
    action      dbg_log_data.action%TYPE,
    debug_mode  BOOLEAN
  );
  TYPE action_stack_type IS TABLE OF action_stack_record INDEX BY PLS_INTEGER;
 
  TYPE call_record IS RECORD
  (
    module    VARCHAR2(100),
    log_level dbg_log_data.log_level%TYPE
  );
  TYPE call_stack_type IS TABLE OF call_record INDEX BY PLS_INTEGER;
 
  TYPE dump_collection IS TABLE OF dbg_log_data%ROWTYPE INDEX BY PLS_INTEGER;
 
  stats_array   stats_collection;
  action_stack  action_stack_type;
  call_stack    call_stack_type;
  dump_array    dump_collection;
 
  v_main_module VARCHAR2(256);
  n_log_level   dbg_log_data.log_level%TYPE;
  n_call_level  PLS_INTEGER;
  n_dump_idx    PLS_INTEGER;
  b_debug       BOOLEAN;
 
  
  PROCEDURE open_log
  (
    p_name IN VARCHAR2,
    p_comment IN CLOB DEFAULT NULL,
    p_debug IN BOOLEAN DEFAULT FALSE
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF g_proc_id IS NULL THEN
      v_main_module := SYS_CONTEXT('USERENV','MODULE');
      
      call_stack.DELETE;
      action_stack.DELETE;
      dump_array.DELETE;
      stats_array.DELETE;
     
      n_call_level := 0;
      n_log_level := 0;
      n_dump_idx := 1;
     
      b_debug := p_debug;
     
      SELECT seq_dbg_xlogger.NEXTVAL INTO g_proc_id FROM dual;
     
      INSERT INTO dbg_process_logs(proc_id, name, comment_txt)
      VALUES(g_proc_id, p_name, p_comment);
     
      COMMIT;
    END IF;
   
    DBMS_APPLICATION_INFO.SET_MODULE(p_name, NULL);
     
    n_call_level := n_call_level+1;
    call_stack(n_call_level).module := p_name;
    call_stack(n_call_level).log_level:= n_log_level;
     
    begin_action(p_name, p_comment, p_debug);
  END;
    
 
  FUNCTION get_current_proc_id RETURN PLS_INTEGER IS
  BEGIN
    RETURN g_proc_id;
  END;
  
  
  PROCEDURE write_log
  (
    p_action    IN VARCHAR2, 
    p_comment   IN CLOB DEFAULT NULL, 
    p_persist   IN BOOLEAN
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    dmp   dbg_log_data%ROWTYPE;
  BEGIN
    IF p_persist THEN
      INSERT INTO dbg_log_data(proc_id, tstamp, log_level, action, comment_txt)
      VALUES(g_proc_id, SYSTIMESTAMP, n_log_level, p_action, p_comment);
      
      COMMIT;
    ELSE
      dmp.proc_id := g_proc_id;
      dmp.tstamp := SYSTIMESTAMP;
      dmp.log_level := n_log_level;
      dmp.action := p_action;
      dmp.comment_txt := p_comment;
    
      dump_array(n_dump_idx) := dmp;
      n_dump_idx := MOD(n_dump_idx,1000)+1;
    END IF;
  END;
 
  
  PROCEDURE begin_action
  (
    p_action    IN VARCHAR2, 
    p_comment   IN CLOB DEFAULT 'Started', 
    p_debug     IN BOOLEAN DEFAULT NULL
  ) IS
    stk   action_stack_record;
  BEGIN
    DBMS_APPLICATION_INFO.SET_ACTION(p_action);
    
    IF g_proc_id IS NOT NULL THEN
      stk.action := p_action;
     
      IF p_debug OR p_debug IS NULL AND b_debug THEN
        stk.debug_mode := TRUE;
      ELSE
        stk.debug_mode := FALSE;
      END IF;
     
      IF stats_array.EXISTS(p_action) AND stats_array(p_action).tstamp IS NOT NULL THEN
        -- This can happen due to a bug in the caller program:
        -- it is not allowed to begin the same action again without ending it first
        Raise_Application_Error(-20000, 'XL.BEGIN_ACTION: action "'||p_action||'" has been already started! This is a bug!');
      ELSE
        -- Mark start of the action and put it into the action stack
        stats_array(p_action).tstamp := SYSTIMESTAMP;
        n_log_level := n_log_level+1;
        action_stack(n_log_level) := stk;
      END IF;
      
      write_log(p_action, p_comment, stk.debug_mode);
    END IF;
  END;
 
  
  PROCEDURE end_action(p_comment IN CLOB DEFAULT 'Completed') IS
    stk   action_stack_record;
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      stk := action_stack(n_log_level); -- get current action from the stack
    
      IF NOT stats_array.EXISTS(stk.action) OR stats_array(stk.action).tstamp IS NULL THEN
        -- This can happen only due to a bug in this program
        Raise_Application_Error(-20000, 'XL.END_ACTION: action "'||stk.action||'" has not been started! This is a bug!');
      END IF;
     
      stats_array(stk.action).cnt := NVL(stats_array(stk.action).cnt, 0) + 1; -- count occurances of this action
      stats_array(stk.action).dur := NVL(stats_array(stk.action).dur, INTERVAL '0' SECOND) + (SYSTIMESTAMP - stats_array(stk.action).tstamp); -- add to total time spent on this action
      stats_array(stk.action).tstamp := NULL; -- mark end of action
    
      write_log(stk.action, p_comment, stk.debug_mode);
      
      n_log_level := n_log_level-1; -- go up by the action stack
      IF n_log_level > 0 THEN
        DBMS_APPLICATION_INFO.SET_ACTION(action_stack(n_log_level).action);
      ELSE
        DBMS_APPLICATION_INFO.SET_ACTION(NULL);
      END IF;
    END IF;
  END;
 
  
  PROCEDURE close_log(p_result IN VARCHAR2 DEFAULT NULL, p_dump IN BOOLEAN DEFAULT FALSE) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  
    act    dbg_log_data.action%TYPE;
  BEGIN
    IF g_proc_id IS NOT NULL THEN -- if logging has been started in this session:
      WHILE n_log_level > call_stack(n_call_level).log_level LOOP
        end_action(p_result);
      END LOOP;
       
      n_call_level := n_call_level-1; -- go up by the call stack:
      IF n_call_level > 0 THEN
        DBMS_APPLICATION_INFO.SET_MODULE(call_stack(n_call_level).module, NULL);
      ELSE
        DBMS_APPLICATION_INFO.SET_MODULE(v_main_module, NULL);
      END IF;
      
      IF n_call_level = 0 THEN -- if this is the end of the main process
        IF p_dump THEN -- if request received to dump debugging information accumulated in memory (usually from an exception handler):
          -- Save log data accumulated in memory:
          FORALL i IN 1..dump_array.COUNT INSERT INTO dbg_log_data VALUES dump_array(i);
        END IF;
      
        -- Save performance statistics accumulated in memory:
        act := stats_array.FIRST;
       
        WHILE act IS NOT NULL LOOP
          INSERT INTO dbg_performance_data(proc_id, action, cnt, seconds)
          VALUES
          (
            g_proc_id, act, stats_array(act).cnt,
            EXTRACT(DAY FROM stats_array(act).dur)*86400 +
            EXTRACT(HOUR FROM stats_array(act).dur)*3600 +
            EXTRACT(MINUTE FROM stats_array(act).dur)*60 +
            EXTRACT(SECOND FROM stats_array(act).dur)
          );
         
          act := stats_array.NEXT(act);
        END LOOP;
       
        UPDATE dbg_process_logs SET end_time = SYSTIMESTAMP, result = p_result
        WHERE proc_id = g_proc_id;
       
        COMMIT;
       
        g_proc_id := NULL;
      END IF;
    END IF;
  END;
  
  
  PROCEDURE spool_log(p_where IN VARCHAR2 DEFAULT NULL, p_max_rows IN PLS_INTEGER DEFAULT 100) IS
    cur     SYS_REFCURSOR;
    whr     VARCHAR2(128);
    line    VARCHAR2(255);
  BEGIN
    whr := NVL(p_where,'comment_txt NOT LIKE ''Started%''');
    
    OPEN cur FOR '
    SELECT * FROM
    (
      SELECT SUBSTR(action||'': ''||comment_txt, 1, 254)
      FROM dbg_log_data
      WHERE proc_id = xl.get_current_proc_id AND '||whr||'
      ORDER BY tstamp
    ) l
    WHERE ROWNUM < :max_rows' USING p_max_rows;
  
    LOOP
      FETCH cur INTO line;
      EXIT WHEN cur%NOTFOUND;
      DBMS_OUTPUT.PUT_LINE(line||CHR(10));
    END LOOP;
    
    CLOSE cur;
  END;
  
  
  PROCEDURE cancel_log IS
  BEGIN
   IF g_proc_id IS NOT NULL THEN
      n_call_level := 1;
      close_log('Cancelled');
    END IF;
  END;
END;
/
