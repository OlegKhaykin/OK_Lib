CREATE OR REPLACE PACKAGE BODY debuger.pkg_dbg_xlogger AS
/*
  This package is for debugging and performance tuning
 
  History of changes - newest to oldest:
  ------------------------------------------------------------------------------
  17-Mar-2020, OK: in CLOSE_LOG, dump in-memory data even if it is not the top-level call
  20-Feb-2020, OK: added column PLS_UNIT to DBG_LOG_DATA;
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
    pls_unit      dbg_log_data.pls_unit%TYPE,
    action        dbg_log_data.action%TYPE,
    log_in_table  BOOLEAN
  );
  TYPE action_stack_type IS TABLE OF action_stack_record INDEX BY PLS_INTEGER;
  
  TYPE call_record IS RECORD
  (
    module    VARCHAR2(100),
    log_depth dbg_log_data.log_depth%TYPE
  );
  TYPE call_stack_type IS TABLE OF call_record INDEX BY PLS_INTEGER;
 
  TYPE dump_collection IS TABLE OF dbg_log_data%ROWTYPE INDEX BY PLS_INTEGER;
 
  stats_array   stats_collection;
  action_stack  action_stack_type;
  call_stack    call_stack_type;
  dump_array    dump_collection;
  
  v_main_module   VARCHAR2(256);
  v_proc_name     dbg_process_logs.name%TYPE;
  r_process_log   dbg_process_logs%ROWTYPE;
  n_proc_id       dbg_process_logs.proc_id%TYPE;
  n_log_level     PLS_INTEGER;
  n_log_depth     dbg_log_data.log_depth%TYPE;
  n_call_depth    PLS_INTEGER;
  n_dump_idx      PLS_INTEGER;
  
  PROCEDURE set_log_level
  (
    p_proc_name         IN VARCHAR2,
    p_session_client_id IN VARCHAR2,
    p_log_level         IN NUMBER
  ) IS
  BEGIN
    DBMS_SESSION.SET_CONTEXT('CTX_GLOBAL_DEBUG', p_proc_name, TO_CHAR(p_log_level), NULL, p_session_client_id);
  END;
  
  PROCEDURE reset_log_level
  (
    p_proc_name         IN VARCHAR2,
    p_session_client_id IN VARCHAR2
  ) IS
  BEGIN
    DBMS_SESSION.CLEAR_CONTEXT('CTX_GLOBAL_DEBUG', p_session_client_id, p_proc_name);
  END;
  
  
  PROCEDURE open_log
  (
    p_name        IN VARCHAR2,
    p_comment     IN CLOB DEFAULT NULL, 
    p_log_level   IN PLS_INTEGER DEFAULT 0,
    p_pls_unit    IN VARCHAR2 DEFAULT NULL,
    p_identify    IN BOOLEAN DEFAULT TRUE
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF n_proc_id IS NULL THEN
      n_proc_id := seq_dbg_xlogger.NEXTVAL;
      v_proc_name := CASE WHEN p_pls_unit IS NOT NULL THEN p_pls_unit||'.' END || p_name;
      
      r_process_log := NULL;
      r_process_log.proc_id := n_proc_id;
      r_process_log.name := v_proc_name;
      r_process_log.comment_txt := p_comment;
      r_process_log.start_time := SYSTIMESTAMP;
      
      IF p_identify THEN
        DBMS_SESSION.SET_IDENTIFIER(r_process_log.proc_id);
      END IF;
      
      v_main_module := SYS_CONTEXT('USERENV','MODULE');
      
      call_stack.DELETE;
      action_stack.DELETE;
      dump_array.DELETE;
      stats_array.DELETE;
      
      n_call_depth := 0;
      n_log_depth := 0;
      n_dump_idx := 1;
      
      SELECT NVL(MAX(log_level), NVL(p_log_level, 0)) INTO n_log_level
      FROM dbg_settings WHERE proc_name = v_proc_name;
      
      IF n_log_level >= 0 THEN
        INSERT INTO dbg_process_logs VALUES r_process_log;
        COMMIT;
      END IF;
    END IF;
    
    DBMS_APPLICATION_INFO.SET_MODULE(p_name, NULL);
    
    n_call_depth := n_call_depth+1;
    call_stack(n_call_depth).module := p_name;
    call_stack(n_call_depth).log_depth := n_log_depth;
     
    begin_action(p_name, p_comment, CASE WHEN n_call_depth = 1 THEN n_log_level ELSE p_log_level END, NVL(p_pls_unit, 'NA'));
  END;
    
  
  FUNCTION get_current_proc_id RETURN PLS_INTEGER IS
  BEGIN
    RETURN n_proc_id;
  END;
  

  PROCEDURE write_suppl_data(p_name IN dbg_supplemental_data.name%TYPE, p_value IN SYS.AnyData) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO dbg_supplemental_data VALUES(n_proc_id, p_name, SYSTIMESTAMP, p_value);
    COMMIT;
  END;
    
  
  PROCEDURE write_log
  (
    p_pls_unit  IN VARCHAR2,
    p_action    IN VARCHAR2, 
    p_comment   IN CLOB DEFAULT NULL, 
    p_persist   IN BOOLEAN
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    
    dmp   dbg_log_data%ROWTYPE;
  BEGIN
    IF p_persist THEN
      INSERT INTO dbg_log_data(proc_id, tstamp, log_depth, pls_unit, action, comment_txt)
      VALUES(n_proc_id, SYSTIMESTAMP, n_log_depth, p_pls_unit, p_action, p_comment);
      
      COMMIT;
    ELSE
      dmp.proc_id := n_proc_id;
      dmp.tstamp := SYSTIMESTAMP;
      dmp.log_depth := n_log_depth;
      dmp.pls_unit := p_pls_unit;
      dmp.action := p_action;
      dmp.comment_txt := p_comment;
    
      dump_array(n_dump_idx) := dmp;
      n_dump_idx := MOD(n_dump_idx,1000)+1;
    END IF;
  END;
 
  
  PROCEDURE begin_action
  (
    p_action      IN VARCHAR2, 
    p_comment     IN CLOB DEFAULT 'Started', 
    p_log_level   IN PLS_INTEGER DEFAULT 1,
    p_pls_unit    IN VARCHAR2 DEFAULT 'NA'
  ) IS
    stk           action_stack_record;
    v_stats_idx   VARCHAR2(255);
  BEGIN
    DBMS_APPLICATION_INFO.SET_ACTION(p_action);
    
    IF n_proc_id IS NOT NULL THEN
      stk.pls_unit := NVL(p_pls_unit, 'NA');
      stk.action := p_action;
      
      IF p_log_level BETWEEN 0 AND NVL(TO_NUMBER(SYS_CONTEXT('CTX_GLOBAL_DEBUG', v_proc_name)), n_log_level) THEN
        stk.log_in_table := TRUE;
      ELSE
        stk.log_in_table := FALSE;
      END IF;

      v_stats_idx := stk.pls_unit||'.'||stk.action;
      IF stats_array.EXISTS(v_stats_idx) AND stats_array(v_stats_idx).tstamp IS NOT NULL THEN
        -- This can happen due to a bug in the caller program:
        -- it is not allowed to begin the same action again without ending it first
        Raise_Application_Error(-20000, 'Action "'||v_stats_idx||'" has been already started! Correct mismatch between XL.BEGIN_ACTION and XL.END_ACTION calls.');
      ELSE
        -- Mark start of the action and put it into the action stack
        stats_array(v_stats_idx).tstamp := SYSTIMESTAMP;
        n_log_depth := n_log_depth+1;
        action_stack(n_log_depth) := stk;
      END IF;
      
      write_log(stk.pls_unit, p_action, p_comment, stk.log_in_table);
    END IF;
  END;
 
  
  PROCEDURE end_action(p_comment IN CLOB DEFAULT 'Completed') IS
    stk           action_stack_record;
    v_stats_idx   VARCHAR2(255);
  BEGIN
    IF n_proc_id IS NOT NULL THEN
      stk := action_stack(n_log_depth); -- get current action from the stack
      v_stats_idx := stk.pls_unit||'.'||stk.action;
    
      IF NOT stats_array.EXISTS(v_stats_idx) OR stats_array(v_stats_idx).tstamp IS NULL THEN
        -- This can happen only due to a bug in this program
        Raise_Application_Error(-20000, 'XL.END_ACTION: action "'||v_stats_idx||'" has not been started! This is a bug in PKG_DBG_XLOGGER!');
      END IF;
      
      stats_array(v_stats_idx).cnt := NVL(stats_array(v_stats_idx).cnt, 0) + 1; -- count occurances of this action
      stats_array(v_stats_idx).dur := NVL(stats_array(v_stats_idx).dur, INTERVAL '0' SECOND) + (SYSTIMESTAMP - stats_array(v_stats_idx).tstamp); -- add to total time spent on this action
      stats_array(v_stats_idx).tstamp := NULL; -- mark end of action
      
      write_log(stk.pls_unit, stk.action, p_comment, stk.log_in_table);
      
      n_log_depth := n_log_depth-1; -- go up by the action stack
      IF n_log_depth > 0 THEN
        DBMS_APPLICATION_INFO.SET_ACTION(action_stack(n_log_depth).action);
      ELSE
        DBMS_APPLICATION_INFO.SET_ACTION(NULL);
      END IF;
    END IF;
  END;
  
  
  PROCEDURE close_log(p_result IN VARCHAR2 DEFAULT NULL, p_dump IN BOOLEAN DEFAULT FALSE) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  
    r_act     dbg_log_data.action%TYPE;
    n_seconds NUMBER;
    
    PROCEDURE set_seconds(p_interval INTERVAL DAY TO SECOND) IS
    BEGIN
      n_seconds := 
      EXTRACT(DAY FROM p_interval)*86400 +
      EXTRACT(HOUR FROM p_interval)*3600 +
      EXTRACT(MINUTE FROM p_interval)*60 +
      EXTRACT(SECOND FROM p_interval);
    END;
  BEGIN
    IF n_proc_id IS NOT NULL THEN -- if logging has been started in this session:
      WHILE n_log_depth > call_stack(n_call_depth).log_depth LOOP
        end_action(p_result);
      END LOOP;
      
      IF p_dump AND dump_array.COUNT > 0 THEN
        FORALL i IN 1..dump_array.COUNT INSERT INTO dbg_log_data VALUES dump_array(i);
        dump_array.DELETE;
        n_dump_idx := 1;
      END IF;
      
      n_call_depth := n_call_depth-1; -- go up by the call stack:
      IF n_call_depth > 0 THEN
        DBMS_APPLICATION_INFO.SET_MODULE(call_stack(n_call_depth).module, NULL);
      ELSE
        DBMS_APPLICATION_INFO.SET_MODULE(v_main_module, NULL);
      END IF;
      
      IF n_call_depth = 0 THEN -- if this is the end of the main process
        r_process_log.result := p_result;
        r_process_log.end_time := SYSTIMESTAMP;
       
        IF n_log_level < 0 THEN
          set_seconds(r_process_log.end_time - r_process_log.start_time);
          
          IF p_dump OR n_seconds >= ABS(n_log_level) THEN
            INSERT INTO dbg_process_logs VALUES r_process_log;
          END IF;
        END IF;
        
        -- Save performance statistics accumulated in memory:
        IF n_log_level >= 0 OR p_dump OR n_seconds >= ABS(n_log_level) THEN
          r_act := stats_array.FIRST;
         
          WHILE r_act IS NOT NULL LOOP
            set_seconds(stats_array(r_act).dur);
            
            INSERT INTO dbg_performance_data(proc_id, action, cnt, seconds)
            VALUES(n_proc_id, r_act, stats_array(r_act).cnt, n_seconds);
            
            r_act := stats_array.NEXT(r_act);
          END LOOP;
        END IF;
        
        IF n_log_level >= 0 THEN
          UPDATE dbg_process_logs SET end_time = SYSTIMESTAMP, result = p_result
          WHERE proc_id = n_proc_id;
        END IF;
        
        n_proc_id := NULL;
      END IF; -- n_call_depth = 0
      
    COMMIT;
    
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
   IF n_proc_id IS NOT NULL THEN
      n_call_depth := 1;
      close_log('Cancelled');
    END IF;
  END;
END;
/
