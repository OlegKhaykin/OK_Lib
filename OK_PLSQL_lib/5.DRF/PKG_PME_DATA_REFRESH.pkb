CREATE OR REPLACE PACKAGE BODY pkg_pme_data_refresh AS
/*
 This package provides a metadata-driven ETL framework
 for automated data refresh in various target DB tables -
 according to the data changes in source tables.
 This is somewhat similar to Oracle materialized view fast refresh mechanism
 except this solution provides more control over data refresh process
 and does not require MV log creation on the source tables.
 
 Controlling metadata is stored in 2 tables:
 - PME_DATA_REFRESH_JOBS;
 - PME_DATA_REFRESH_LIST;

 Change history - from newest to oldest:
 -------------------------------------------------------------------------------
 01-Feb-2013, OK:
 - bug fix in MASTER, replaced hard-coded "DM_REFRESH"
                      with the name of the current refresh process;

 - Implemented "coalesce" logic in GATHER_ERROR_STATS;   
 
 14-Jan-2013, OK: improved error marking algorythm in SLAVE;  
  
 10-Jan-2013, OK:
 - MASTER: simpler job assignment distribution if MAX_SLAVE_NUM=1;
 - SLAVE: started using column HINT of the refresh list; 
 
 09-Jan-2013, OK: minor bug fix in MASTER (error counting at the end); 
 
 08-Jan-2013, OK: bug fix in STOP_PROCESS;
 
 07-Jan-2013, OK: added logging to the STOP_PROCESS procedure;
 
 11-Dec-2012, OK: added procedure PURGE_ERRORS;
 
 30-Nov-2012, OK: added procedure GATHER_ERROR_STATS;

 26-Nov-2012, OK: added parameter P_SLAVE_ID to EXEC_TASK procedure;

 18-Nov-2012, OK: added parameter P_ERROR_LOG_TABLE to SETUP procedure;
 
 15-Nov-2012, OK: implemented logic of using MAX_NUM_OF_JOBS parameter;
 
 9-Nov-2012, OK: added iteration TS range into the Master log;
 
 2-Nov-2012, OK: fixed bug in SLAVE;
 
 21-Oct-2012, OK, in SLAVE:
 - added possibility of asynchronous step start if AS_JOB='Y';
 - added processing of operations "WAIT" and "PROCEDURE"; 
 
 19-Oct-2012, OK: added processing of 'NULL' in SETUP;
 
 09-Oct-2012, OK: removed hint "cardinality" in SLAVE;
 
 30-Sep-2012, OK:
 - Removed any usage of REFRESH_TYPE column that may exist in a Job Assignment Table.
   That column should be removed from PKs of all Job Assignment tables.
 - Added parameter P_SET_CTX_PROC to SETUP procedure;
 - Added SET_CONTEXT_PROCEDURE processing to MASTER procedure;
 - Added procedure HEARTBEAT;
 - Increased delay tolerance in STOP_PROCESS procedure;
 
 30-Jul-2012, OK:
 - In SLAVE: used hint "cardinality";
 - In MASTER: set UPDATE_DTIME 5 minutes ahead before calculating Job Assignments; 
 
 25-Jul-2012, OK: added procedures START_PROCESS and STOP_PROCESS;
 
 . . .
 
 15-Dec-2011, OK: created original version;
*/
  MAX_SLEEP_SECONDS CONSTANT PLS_INTEGER := 5;
  
  g_slave_num   PLS_INTEGER;
  g_min_ts      TIMESTAMP;
  g_max_ts      TIMESTAMP;
  
  PROCEDURE set_min_ts(p_ts IN TIMESTAMP) IS
  BEGIN
    g_min_ts := p_ts;
  END;
  
  PROCEDURE set_slave_num(p_num IN PLS_INTEGER) IS -- this is mostly for testing purposes
  BEGIN
    g_slave_num := p_num;
  END;
  
  -- Procedure to create or change a refresh job
  PROCEDURE setup
  (
    p_refresh_type            IN VARCHAR2, 
    p_signal                  IN VARCHAR2 DEFAULT NULL, 
    p_data_change_audit_view  IN VARCHAR2 DEFAULT NULL, 
    p_job_assignment_table    IN VARCHAR2 DEFAULT NULL, 
    p_error_log_table          IN VARCHAR2 DEFAULT NULL,
    p_batch_size              IN NUMBER DEFAULT NULL, 
    p_max_num_of_slaves       IN NUMBER DEFAULT NULL, 
    p_sleep_time_in_seconds   IN NUMBER DEFAULT NULL, 
    p_overlap_seconds         IN NUMBER DEFAULT NULL,
    p_before_proc             IN VARCHAR2 DEFAULT NULL,
    p_after_proc              IN VARCHAR2 DEFAULT NULL,
    p_last_processed_change_ts IN TIMESTAMP DEFAULT NULL 
  ) IS
  BEGIN
    MERGE INTO pme_data_refresh_jobs j
    USING
    (
      SELECT 
        p_refresh_type AS refresh_type, 
        p_data_change_audit_view AS data_change_audit_view,
        p_job_assignment_table AS job_assignment_table, 
        p_error_log_table AS error_log_table, 
        p_batch_size AS batch_size,
        p_max_num_of_slaves AS max_num_of_slaves, 
        p_sleep_time_in_seconds AS sleep_time_in_seconds,
        p_overlap_seconds AS overlap_seconds, 
        p_signal AS signal,
        p_before_proc AS before_proc,
        p_after_proc AS after_proc,
        p_last_processed_change_ts AS last_processed_change_ts
      FROM dual
    ) q
    ON (j.refresh_type = q.refresh_type)
    WHEN MATCHED THEN UPDATE SET
      j.signal = NVL(q.signal, j.signal),
      j.data_change_audit_view = NVL(q.data_change_audit_view, j.data_change_audit_view),
      j.job_assignment_table = NVL(q.job_assignment_table, j.job_assignment_table),
      j.error_log_table = DECODE(q.error_log_table, NULL, j.error_log_table, 'NULL', NULL, q.error_log_table),
      j.batch_size = NVL(q.batch_size, j.batch_size),
      j.max_num_of_slaves = NVL(q.max_num_of_slaves, j.max_num_of_slaves),
      j.sleep_time_in_seconds = NVL(q.sleep_time_in_seconds, j.sleep_time_in_seconds),
      j.overlap_seconds = NVL(q.overlap_seconds, j.overlap_seconds),
      j.before_proc = DECODE(q.before_proc, NULL, j.before_proc, 'NULL', NULL, q.before_proc),
      j.after_proc = DECODE(q.after_proc, NULL, j.after_proc, 'NULL', NULL, q.after_proc),
      j.last_processed_change_ts = NVL(q.last_processed_change_ts, j.last_processed_change_ts),
      j.update_dtime = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT
    (
      refresh_type, data_change_audit_view,
      job_assignment_table, error_log_table,
      batch_size, max_num_of_slaves, 
      sleep_time_in_seconds, overlap_seconds,
      before_proc, after_proc, signal,
      last_processed_change_ts, update_dtime 
    )
    VALUES
    (
      p_refresh_type, 
      p_data_change_audit_view, 
      p_job_assignment_table,
      p_error_log_table,
      NVL(p_batch_size, 500),
      NVL(p_max_num_of_slaves, 5), 
      NVL(p_sleep_time_in_seconds, 300),
      NVL(p_overlap_seconds, 0),
      p_before_proc, p_after_proc,
      NVL(p_signal, 'STOP'), 
      NVL(p_last_processed_change_ts, TIMESTAMP '0001-01-01 00:00:00'), 
      SYSTIMESTAMP 
    );
    
    COMMIT;  
  END; 

  FUNCTION get_slave_num RETURN PLS_INTEGER IS
  BEGIN
    RETURN g_slave_num;
  END;
  
  FUNCTION get_min_ts RETURN TIMESTAMP IS
  BEGIN
    RETURN NVL(g_min_ts, TIMESTAMP '1-1-1 00:00:00');
  END;
  
  -- Procedure to start the refresh process
  PROCEDURE start_process(p_refresh_type IN VARCHAR) IS
  BEGIN
    master(p_refresh_type);
  END;

  -- Procedure to stop the refresh process
  PROCEDURE stop_process(p_refresh_type IN VARCHAR) IS
    udt   DATE;
    edt   DATE;
    cnt   PLS_INTEGER;
    res   dbg_process_logs.result%TYPE;
    pid   dbg_process_logs.proc_id%TYPE;
  BEGIN
    xl.open(p_refresh_type||'_STOP', TRUE);
    
    SELECT COUNT(1), MAX(proc_id) INTO cnt, pid
    FROM pme_data_refresh_jobs
    WHERE refresh_type = p_refresh_type;
    
    IF cnt = 0 THEN
      Raise_Application_Error(-20000, 'Wrong refresh type: '||p_refresh_type);
    END IF;
    
    setup(p_refresh_type, 'STOP');
    
    IF pid IS NOT NULL THEN
      LOOP
        dbms_lock.sleep(MAX_SLEEP_SECONDS);
        
        BEGIN
          SELECT j.update_dtime, l.end_time, l.result
          INTO udt, edt, res
          FROM pme_data_refresh_jobs j
          JOIN dbg_process_logs l ON l.proc_id = j.proc_id
          WHERE j.refresh_type = p_refresh_type;
        EXCEPTION
         WHEN NO_DATA_FOUND THEN
          Raise_Application_Error(-20000, 'Process log is missing, PROC_ID='||pid||'.');
        END;
          
        IF edt IS NOT NULL THEN
          dbms_output.put_line('"'||p_refresh_type||'": '||res);
          EXIT;
        END IF;
          
        IF udt < (SYSDATE - (2*MAX_SLEEP_SECONDS+1)/86400) THEN
          Raise_Application_Error(-20000, '"'||p_refresh_type||'" process seems to be dead.');
        END IF;
      END LOOP;
    END IF;
    
    xl.close('Successfylly completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.close(SQLERRM);
    RAISE;
  END;
  
  -- Procedure HEARTBEAT updates UPDATE_DTIME in the PME_DATA_REFRESH_JOBS
  PROCEDURE heartbeat(p_refresh_type IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE pme_data_refresh_jobs SET update_dtime = SYSDATE
    WHERE refresh_type = p_refresh_type;
    COMMIT;
  END;
  
  FUNCTION get_key_column(p_refresh_type IN VARCHAR2) RETURN VARCHAR2 IS
    drv           pme_data_refresh_jobs%ROWTYPE;
    action        dbg_log_data.action%TYPE;
    job_schema    VARCHAR2(30);
    job_assgn_tab VARCHAR2(30);
    db_link       VARCHAR2(30);
    key_col       VARCHAR2(30);
  BEGIN
    action := 'Finding the key column';
    xl.write(action, 'Started');
    
    SELECT * INTO drv FROM pme_data_refresh_jobs WHERE refresh_type = p_refresh_type;
    
    utl.get_schema_table(drv.job_assignment_table, job_schema, job_assgn_tab, db_link);
        
    SELECT cc.column_name INTO key_col
    FROM all_constraints c 
    JOIN all_cons_columns cc ON cc.owner = c.owner
     AND cc.constraint_name = c.constraint_name
    WHERE c.owner = job_schema
    AND c.table_name = job_assgn_tab
    AND c.constraint_type = 'P';
        
    IF key_col IS NULL THEN
      Raise_Application_Error(-20000, 'No proper PK specified for '||drv.job_assignment_table);
    END IF;
    xl.write(action, key_col||' is the key column.');
    
    RETURN key_col;    
  END;
        
  -- Procedure MASTER.
  -- --------------------------------------------------------------------------
  -- Procedure MASTER must be started with the parameter P_REFRESH_TYPE equal to
  -- one of the REFRESH_TYPE values from the PME_DATA_REFRESH_JOBS table.
  --
  -- Here is the logic of the MASTER procedure:
  -- 1) Get configiuration settings from the PME_DATA_REFRESH_JOBS row;
  --    If SIGNAL='START' then continue; otherwise - exit. 
  -- 2) Read new entries from the change audit view whose name is given in
  --    in the CHANGE_AUDIT_VIEW column of the PME_DATA_REFRESH_JOBS table.
  --    If there are no new changes then procced to Step 6.
  -- 3) For every distinct ENTITY_ID listed in the change audit view,
  --    put one row into the Job Assigments table, whose name is given in the
  --    PME_DATA_REFRESH_JOBS.
  -- 4) Start the required number of "slave" jobs to process the
  --    generated Job Assignments;
  -- 5) Wait until the "slave" jobs end. At this moment, no entries should be left
  --    in Job Assignments table except those that have errored-out.
  --    If there are some entries left - raise an exception and exit.
  -- 6) Check the value of SLEEP_TIME_IN_SECONDS. If it is grater than 0 then 
  --    "sleep" for that long and then start the loop from Step 1; 
  --    otherwise - exit with success.
  PROCEDURE master(p_refresh_type IN VARCHAR2) IS
    action        dbg_log_data.action%TYPE;
    drv           pme_data_refresh_jobs%ROWTYPE;
    key_col       VARCHAR2(100);
    cmd           VARCHAR2(4000);
    n_slaves      PLS_INTEGER;
    cnt           PLS_INTEGER;
    err_cnt       PLS_INTEGER;
    starting      BOOLEAN := TRUE;
    next_start    DATE;
    sleep_seconds PLS_INTEGER;
  BEGIN
    SELECT COUNT(1) INTO cnt FROM dbg_process_logs
    WHERE description = p_refresh_type||'_M' AND end_time IS NULL;
    IF cnt > 0 THEN
      Raise_Application_Error
      (
        -20000, 
        'The process "'||p_refresh_type||'" seems to be running already. '||
        'If so, do not try to start another instance of it. '||
        'If this is not the case, find in the DBG_PROCESS_LOGS table '||
        'all the rows where DESCRIPTION='''||p_refresh_type||'_M'' '||
        'and END_TIME is NULL. Set END_TIME to a not-NULL value and try again.'
      );
    END IF;

    xl.open(p_refresh_type||'_M', TRUE);
    
    UPDATE pme_data_refresh_jobs SET
      signal = 'START',
      proc_id = xl.get_current_proc_id,
      update_dtime = SYSDATE
    WHERE refresh_type = p_refresh_type;
    
    LOOP
      action := 'Getting configuration parameters for "'||p_refresh_type||'"';
      xl.write(action, 'Started');
      BEGIN
        SELECT * INTO drv
        FROM pme_data_refresh_jobs
        WHERE refresh_type = p_refresh_type;
      EXCEPTION
       WHEN NO_DATA_FOUND THEN
        Raise_Application_Error(-20000, 'Data Refresh Type "'||p_refresh_type||
        '": is not registered in PME_DATA_REFRESH_JOBS');
      END;
      
      IF drv.sleep_time_in_seconds > 0 THEN
        next_start := SYSDATE + drv.sleep_time_in_seconds/86400;
      END IF;
      g_min_ts := drv.last_processed_change_ts - drv.overlap_seconds/86400;
      xl.write(action, 'Done');
      
      action := 'Setting Refresh Context';
      xl.write(action, 'Started');
      
      IF drv.before_proc IS NULL THEN
        dbms_session.set_identifier(TO_CHAR(g_min_ts, 'YYYY-MM-DD HH24:MI:SS'));
      ELSE
        EXECUTE IMMEDIATE 'BEGIN '||drv.before_proc||'(:p_since); END;'
        USING g_min_ts;
      END IF;
      
      xl.write(action, 'Done');
      
      IF starting THEN
        key_col := get_key_column(p_refresh_type);
        
        action := 'Purging-out old Job Assignments';
        xl.write(action, 'Started');
        cmd := 'DELETE FROM '||drv.job_assignment_table||'
        WHERE status = ''NEW''';
        EXECUTE IMMEDIATE cmd;
        xl.write(action, 'Done');
        starting := FALSE;
      END IF;
      
      IF drv.signal <> 'START' THEN
        xl.write(action, 'Signal "'||drv.signal||'" received');
        EXIT;
      END IF;
      
      action := 'Looking for new data changes';
      xl.write(action, 'Started');
      
      UPDATE pme_data_refresh_jobs SET update_dtime = SYSDATE + 5/1440 -- 5 min
      WHERE refresh_type = p_refresh_type;
      COMMIT;
      
      utl.add_data
      (
        p_operation => 'MERGE', 
        p_tgt => drv.job_assignment_table,
        p_src => drv.data_change_audit_view,
        p_errtab => drv.error_log_table,
        p_commit_at => -1,
        p_add_cnt => cnt,
        p_err_cnt => err_cnt 
      );
      xl.write(action, cnt||' changes found, '||err_cnt||' errors logged');
      
      IF cnt > 0 THEN
        action := 'Preparing errored-out Job Assignments for re-processing';
        xl.write(action, 'Started');
        cmd := '
        UPDATE '||drv.job_assignment_table||' SET status = ''NEW''
        WHERE status = ''ERROR''';
        EXECUTE IMMEDIATE cmd;
        cnt := SQL%ROWCOUNT;
        xl.write(action, cnt||' Job Assignments will be re-processed');
        
        action := 'Finding time range for this iteration';
        xl.write(action, 'Started');
        cmd := '
        SELECT
          COUNT(DECODE(status, ''NEW'', 1)),
          COUNT(CASE WHEN status = ''IGNORE'' AND slave_num > 0 THEN 1 END),
          MAX(entry_ts)
        FROM '||drv.job_assignment_table||'
        WHERE status IN (''NEW'',''IGNORE'')';
        EXECUTE IMMEDIATE cmd INTO cnt, err_cnt, g_max_ts;
        IF err_cnt >0 THEN
          Raise_Application_Error(-20000, err_cnt||'Job Assignments have status ''IGNORE'' but are still assigned to Slaves');
        END IF;
        xl.write
        (
          action, 'MIN_TS: '||TO_CHAR(g_min_ts, 'YYYY-MM-DD HH24:MI:SS')||
          ', MAX_TS: '||TO_CHAR(g_max_ts, 'YYYY-MM-DD HH24:MI:SS')
        );
        
        action := 'Distributing Job Assignments among "slave" jobs';
        xl.write(action, 'Started');
        
        drv.batch_size := GREATEST(drv.batch_size, CEIL(cnt/drv.max_num_of_slaves));
        n_slaves := CEIL(cnt/drv.batch_size);
        
        IF n_slaves = 1 THEN
          cmd := 'UPDATE '||drv.job_assignment_table||' SET slave_num = :n_slaves
          WHERE status = ''NEW''';
        ELSE
          cmd := '
          MERGE INTO '||drv.job_assignment_table||' t
          USING
          (
            SELECT '||key_col||', ROWNUM rnum
            FROM '||drv.job_assignment_table||'
            WHERE status = ''NEW''
          ) q
          ON (t.'||key_col||' = q.'||key_col||')
          WHEN MATCHED THEN UPDATE SET slave_num = MOD(q.rnum, :n_slaves)+1';
        END IF;
        
        EXECUTE IMMEDIATE cmd USING n_slaves;
        cnt := SQL%ROWCOUNT;
        COMMIT;
        xl.write(action, cnt||' Job Assignments distributed among '||n_slaves||' "slave" jobs');
        
        action := 'Starting "slave" jobs to process the Job Assignments';
        xl.write(action, 'Started');
        FOR slave_num IN 1..n_slaves LOOP
          start_data_refresh_slave
          (
            p_assignment_table => drv.job_assignment_table,
            p_refresh_type => p_refresh_type,
            p_slave_number => slave_num,
            p_key_column => key_col
          );
        END LOOP;
        dbms_lock.sleep(2);
        xl.write(action, 'Done');
        
        utl.wait_for_jobs(p_refresh_type||'_S%', 1, 'pkg_pme_data_refresh.heartbeat('''||p_refresh_type||''')');
        
        IF drv.after_proc IS NOT NULL THEN
          EXECUTE IMMEDIATE 'BEGIN '||REPLACE(drv.after_proc, '''','''''')||'; END;';
        END IF;
        
        action := 'Checking that all the "'||p_refresh_type||'" Job Assignments have been processed';
        xl.write(action, 'Started');
        cmd := '
        SELECT
          COUNT(DECODE(status, ''NEW'', 1)),
          COUNT(DECODE(status, ''ERROR'', 1)) 
        FROM '||drv.job_assignment_table;
        EXECUTE IMMEDIATE cmd INTO cnt, err_cnt;
        IF cnt > 0 THEN
          Raise_Application_Error(-20000, 
          'Not all job assignments have been processed. '||
          'Investigate and resolve before re-starting the "'||p_refresh_type||'" process.');
        END IF;
        IF err_cnt = 0 THEN
          xl.write(action, 'All Job Assignments have been processed');
        ELSE
          xl.write(action, err_cnt||' Job Assignments have been marked as errors');
        END IF;
        action := NULL;
        
        drv.last_processed_change_ts := g_max_ts;
      END IF;
      
      UPDATE pme_data_refresh_jobs SET
        last_processed_change_ts = drv.last_processed_change_ts,
        update_dtime = SYSDATE
      WHERE refresh_type = p_refresh_type;
      COMMIT;
      
      IF drv.sleep_time_in_seconds > 0 THEN
        action := 'Sleeping until '||TO_CHAR(next_start, 'YYYY-MM-DD HH24:MI:SS');
        xl.write(action, 'Started');
        
        WHILE SYSDATE < next_start
        LOOP
          sleep_seconds := LEAST((next_start - SYSDATE)*86400, MAX_SLEEP_SECONDS);
          IF sleep_seconds > 0 THEN
            dbms_lock.sleep(sleep_seconds);
          END IF;
          
          UPDATE pme_data_refresh_jobs SET update_dtime = SYSDATE
          WHERE refresh_type = p_refresh_type;
          COMMIT;
        END LOOP;
        xl.write(action, 'Done');
      ELSE
        EXIT;
      END IF;
    END LOOP;
    
    xl.close('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    IF action IS NOT NULL THEN
      xl.write(action, SQLERRM);
    END IF;

    IF cmd IS NOT NULL THEN
      xl.log_error(cmd);
    END IF;

    xl.close(SQLERRM, TRUE);
    RAISE; 
  END master;
  
  PROCEDURE slave
  (
    p_assignment_table  IN VARCHAR2,
    p_refresh_type      IN VARCHAR2,
    p_slave_number      IN PLS_INTEGER,
    p_key_column        IN VARCHAR2
  ) IS
    n_proc_id   dbg_process_logs.proc_id%TYPE;
    action      dbg_log_data.action%TYPE;
    job_name    VARCHAR2(30);
    cmd         VARCHAR2(30000);
    cnt         PLS_INTEGER;
    ownr        VARCHAR2(30);
    tname       VARCHAR2(30);
    dlink       VARCHAR2(30);
    max_jobs    PLS_INTEGER;
  BEGIN
    xl.open(p_refresh_type||'_S'||p_slave_number, TRUE);
    
    n_proc_id := xl.get_current_proc_id;
    g_slave_num := p_slave_number;
    
    DBMS_SESSION.SET_IDENTIFIER(n_proc_id); -- OK-2012-04-04: to be used as PROC_ID in ERR_* tables
    
    action := 'Checking how many Jobs are available';
    xl.write(action, 'Started');
    
    SELECT max_num_of_jobs INTO max_jobs
    FROM pme_data_refresh_jobs
    WHERE refresh_type = p_refresh_type;
    
    IF max_jobs > 0 THEN
      xl.write(action, max_jobs ||' Jobs are available');
    ELSE
      xl.write(action, 'Unlimited number of Jobs are available');
    END IF; 
    
    FOR r IN
    (
      SELECT rl.* 
      FROM pme_data_refresh_list rl
      WHERE refresh_type = p_refresh_type
      ORDER BY batch_num, num
    )
    LOOP
      action := 'BATCH #'||r.batch_num||', STEP #'||r.num||', OPERATION: '||r.operation||', TGT: '||r.tgt||', AS_JOB='||r.as_job;
      xl.write(action, 'Started');
      
      IF r.operation = 'WAIT' THEN
        FOR w IN
        (
          SELECT VALUE(t) pattern
          FROM TABLE(split_string(r.tgt)) t
        )
        LOOP
          utl.wait_for_jobs(p_refresh_type||'_S'||p_slave_number||'_'||w.pattern, 1);
        END LOOP;
        
      ELSE
        cmd := 
        CASE r.operation
         WHEN 'PROCEDURE' THEN 
          r.tgt||'('''||p_refresh_type||''','||p_slave_number||');'
         WHEN 'DELETE' THEN
         'utl.delete_data(p_tgt=>'''||r.tgt||''', p_src=>'''||r.src||
         ''', p_whr=>'''||r.whr||''', p_hint=>'''||r.hint||''', p_commit_at=>'||r.commit_at||');'
         ELSE -- r.operation IN ('INSERT','APPEND','MERGE' 'REPLACE','EQUALIZE'):
         'utl.add_data(p_operation=>'''||r.operation||''', p_tgt=>'''||r.tgt||
         ''', p_src=>'''||r.src||''', p_whr=>'''||r.whr||''', p_hint=>'''||r.hint||
         ''', p_errtab=>'''||r.err||''', p_commit_at=>'||r.commit_at||');'
        END;
        
        IF r.as_job = 'N' THEN
          cmd := 'BEGIN '||cmd||' END;';
          EXECUTE IMMEDIATE cmd;
        ELSE
          job_name := p_refresh_type||'_S'||p_slave_number||'_B'||r.batch_num||'_'||r.num;
          
          IF max_jobs > 0 THEN
            utl.wait_for_jobs(p_refresh_type||'_S%', max_jobs);
          END IF;
          
          sp_start_job
          (
            job_name, 
            'pkg_pme_data_refresh.exec_task('||n_proc_id||','||p_slave_number||
            ','''||job_name||''','''||REPLACE(cmd,'''','''''')||''');'
          );
        END IF;
        cmd := NULL;
      END IF;
      xl.write(action, 'Completed');
    END LOOP;
    
    action := 'Checking that all child processes completed successfully';
    xl.write(action, 'Started');
    SELECT COUNT(1) INTO cnt
    FROM dbg_process_logs
    WHERE proc_id > xl.get_current_proc_id
    AND description LIKE p_refresh_type||'_S'||p_slave_number||'\_%' ESCAPE '\'
    AND result <> 'Successfully completed';
    IF cnt > 0 THEN
      Raise_Application_Error(-20000, cnt||' '||p_refresh_type||'_S'||p_slave_number||'* processes failed');
    END IF;
    xl.write(action, 'Done');
    
    action := 'Checking for failed Job Assignments';
    xl.write(action, 'Started');
    FOR r IN
    (
      SELECT DISTINCT rl.tgt, rl.err
      FROM pme_data_refresh_list rl
      WHERE rl.refresh_type = p_refresh_type
      AND rl.operation IN ('APPEND', 'INSERT', 'MERGE', 'REPLACE', 'EQUALIZE')
      AND rl.err IS NOT NULL
    )
    LOOP
      utl.get_schema_table(r.err, ownr, tname, dlink);
      
      SELECT COUNT(1) INTO cnt FROM all_tab_columns
      WHERE owner = ownr AND table_name = tname AND column_name = p_key_column;
      
      IF cnt = 1 THEN
        cmd := CASE WHEN cmd IS NOT NULL THEN cmd ||'
        UNION ' END || '
        SELECT DISTINCT '||p_key_column||'
        FROM '||r.err||' 
        WHERE proc_id = '||n_proc_id;
      END IF;
    END LOOP;
    
    IF cmd IS NOT NULL THEN
      cmd := 'MERGE INTO '||p_assignment_table||' t
      USING ('||cmd||') q
      ON (t.'||p_key_column||' = q.'||p_key_column||')
      WHEN MATCHED THEN UPDATE SET status = ''ERROR''';
      
      EXECUTE IMMEDIATE cmd;
      cnt := SQL%ROWCOUNT;
      xl.write(action, cnt||' Job Assignments have been marked as ERRORs');
    ELSE
      xl.write(action, 'All Job Assignments have been successfully processed');
    END IF;
    
    action := 'Deleting successfully processed Job Assignments';
    xl.write(action, 'Started');
    cmd := 'DELETE FROM '||p_assignment_table||'
    WHERE slave_num = :p_slave_number
    AND status <> ''ERROR''';
    EXECUTE IMMEDIATE cmd USING p_slave_number;
    cnt := SQL%ROWCOUNT;
    xl.write(action, cnt||' Job Assignments deleted');
    
    COMMIT; 
    xl.close('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    ROLLBACK;
    IF action IS NOT NULL THEN
      xl.write(action, SQLERRM);
    END IF;
    
    IF cmd IS NOT NULL THEN
      xl.log_error(cmd);
    END IF;
    xl.close(SQLERRM, TRUE);
  END;
  
  -- This procedure is started by SLAVE using DBMS_SCHEDULER to execute one step of the refresh process  
  PROCEDURE exec_task
  (
    p_slave_id      IN NUMBER,
    p_slave_number  IN PLS_INTEGER,
    p_job_name      IN VARCHAR2,
    p_task          IN VARCHAR2
  ) IS
  BEGIN
    xl.open(p_job_name, TRUE);
    dbms_session.set_identifier(p_slave_id);
    g_slave_num := p_slave_number;
    EXECUTE IMMEDIATE 'BEGIN '||p_task||' END;';
    xl.close('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.log_error(p_task);
    xl.close(SQLERRM, TRUE);
  END;
  
  PROCEDURE gather_error_stats(p_refresh_type IN VARCHAR2, p_coalesce IN BOOLEAN DEFAULT FALSE) IS
    act     dbg_log_data.action%TYPE;
    key_col VARCHAR2(30);
    cmd     VARCHAR2(2000);
    cnt     PLS_INTEGER;
  BEGIN
    xl.open(p_refresh_type||'_ERR_STATS', true);
    
    key_col := LOWER(get_key_column(p_refresh_type));
    
    for r in
    (
      SELECT DISTINCT err 
      FROM pme_data_refresh_list
      WHERE refresh_type = p_refresh_type
      AND err IS NOT NULL
    )
    LOOP
      IF p_coalesce THEN
        act := 'Removing duplicate errors from '||r.err;
        xl.write(act, 'Started');
        cmd := '
        DELETE FROM '||r.err||' err
        WHERE EXISTS
        (
          SELECT 1 FROM '||r.err||'
          WHERE '||key_col||' = err.'||key_col||' AND ora_err_mesg$ = err.ora_err_mesg$
          AND ora_err_tag$ < err.ora_err_tag$
        )';
        EXECUTE IMMEDIATE cmd;
        cnt := SQL%ROWCOUNT;
        xl.write(act, cnt||' rows deleted');
      END IF;
      
      act := 'Looking for errors in '||r.err;
      xl.write(act, 'Started');
      
      cmd := '
      MERGE INTO pme_data_refresh_error_stats t
      USING
      (
        SELECT
          '''||p_refresh_type||''' refresh_type, 
          s.err_table,
          s.err_message,
          COUNT(er.ora_err_mesg$) err_cnt
        FROM
        (
          SELECT err_table, err_message
          FROM pme_data_refresh_error_stats es
          WHERE es.refresh_type = '''||p_refresh_type||'''
          AND es.err_table = '''||r.err||'''
          UNION
          SELECT '''||r.err||''', ora_err_mesg$
          FROM '||r.err||'
        ) s 
        LEFT JOIN '||r.err||' er
          ON er.ora_err_mesg$ = s.err_message
        GROUP BY s.err_table, s.err_message
      ) q
      ON 
      (
        t.refresh_type = q.refresh_type AND
        t.err_table = q.err_table AND
        t.err_message = q.err_message
      )
      WHEN MATCHED THEN UPDATE SET t.new_count = q.err_cnt
      WHEN NOT MATCHED THEN INSERT(refresh_type, err_table, err_message, new_count)
      VALUES(q.refresh_type, q.err_table, q.err_message, q.err_cnt)';
      
      EXECUTE IMMEDIATE cmd;
      xl.write(act, 'Done');
      
      DELETE FROM pme_data_refresh_error_stats WHERE new_count = 0 and old_count = 0;
    END LOOP;
    
    COMMIT;
    
    xl.close('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    IF cmd IS NOT NULL THEN
      xl.log_error(cmd);
    END IF;
    xl.close(SQLERRM);
    RAISE;
  END;
  
  PROCEDURE purge_errors(p_refresh_type IN VARCHAR2, p_err_tables IN VARCHAR2 DEFAULT NULL) IS
    err_tables  V2_ARRAY;
    errt        VARCHAR2(30);
  BEGIN
    IF p_err_tables IS NOT NULL then
      SELECT DISTINCT err BULK COLLECT INTO err_tables
      FROM pme_data_refresh_list 
      WHERE refresh_type = UPPER(p_refresh_type)
      AND err IN 
      (
        SELECT VALUE(t)
        FROM TABLE(split_string(p_err_tables)) t
      );
    ELSE
      SELECT DISTINCT err BULK COLLECT INTO err_tables
      FROM pme_data_refresh_list
      WHERE refresh_type = p_refresh_type
      AND err IS NOT NULL;
    END IF;
    
    FOR i IN 1..err_tables.COUNT LOOP
      errt := err_tables(i);
      EXECUTE IMMEDIATE 'TRUNCATE TABLE '||errt;
    END LOOP; 
  EXCEPTION
   WHEN OTHERS THEN
    raise_application_error(-20000, errt||': '||SQLERRM);
  END;
END;
/