CREATE OR REPLACE PACKAGE BODY pkg_data_refresh AS
/*
 This package provides a metadata-driven ETL framework
 for data refresh in various local DB tables
 based on the data coming from source tables that may be either local or remote.
 
 Controlling metadata is stored in 2 tables:
 - CNF_DATA_FLOWS;
 - CNF_DATA_FLOW_STEPS;
 
 Error statistics is stored in LOG_DRF_ERRORS.
 
 30-Mar-2018, OK: new simplified version;
*/
  MAX_SLEEP_SECONDS CONSTANT PLS_INTEGER := 5;
  
  
  -- Procedure HEARTBEAT updates HEARTBEAT_DT in CNF_DATA_FLOWS
  PROCEDURE heartbeat(p_data_flow_cd IN VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE cnf_data_flows SET heartbeat_dt = SYSDATE WHERE data_flow_cd = p_data_flow_cd;
    COMMIT;
  END;
  
  
  -- Procedure START_JOB starts a Scheduler Job executing the given Task
  PROCEDURE start_job(p_job_name IN VARCHAR2, p_task IN VARCHAR2) IS
  BEGIN
    DBMS_SCHEDULER.CREATE_JOB 
    (
      job_name => p_job_name,
      job_style => 'LIGHTWEIGHT',
      program_name => 'EXEC_TASK',
      enabled => FALSE
    );

    DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
    (
      job_name => p_job_name,
      argument_name => 'P_JOB_NAME',
      argument_value => p_job_name
    );

    DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
    (
      job_name => p_job_name,
      argument_name => 'P_TASK',
      argument_value => p_task
    );

    DBMS_SCHEDULER.ENABLE(p_job_name);
  END;
  
  
  -- This procedure is started by DBMS_SCHEDULER to execute the task  
  PROCEDURE exec_task
  (
    p_job_name      IN VARCHAR2,
    p_task          IN VARCHAR2
  ) IS
  BEGIN
    xl.open_log(p_job_name, 'Executing the Data Flow Task', TRUE);
    
    xl.begin_action('Executing task', p_task, FALSE);
    EXECUTE IMMEDIATE p_task;
    xl.end_action;
    
    xl.close_log('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.close_log(SQLERRM, TRUE);
  END;
  
  
  -- Procedure WAIT_FOR_JOBS waits until the number of running Scheduler Jobs
  -- with the given name pattern drops below the given upper limit.
  -- It also checks that all those Jobs have completed successfully.
  PROCEDURE wait_for_jobs(p_pattern IN VARCHAR2, p_upper_limit IN NUMBER) IS
    n_cnt PLS_INTEGER;
  BEGIN
    xl.begin_action('Waiting for the Jobs to complete', p_pattern);
    LOOP
      SELECT COUNT(1) INTO n_cnt
      FROM user_scheduler_jobs
      WHERE job_name LIKE p_pattern;
      
      EXIT WHEN n_cnt < p_upper_limit;
      
      DBMS_LOCK.SLEEP(MAX_SLEEP_SECONDS);
    END LOOP;
    xl.end_action;
    
    xl.begin_action('Checking JOB completion status', 'Started', FALSE);
    SELECT COUNT(1) INTO n_cnt
    FROM dbg_process_logs
    WHERE proc_id > xl.get_current_proc_id
    AND name LIKE p_pattern AND result <> 'Successfully completed';
    
    IF n_cnt > 0 THEN
      Raise_Application_Error(-20000, n_cnt ||' Jobs have failed');
    END IF;
    xl.end_action;
  END;
  
  
  -- Procedure START_DATA_FLOW implements the main Data Flow process.
  PROCEDURE start_data_flow(p_data_flow_cd IN VARCHAR) IS
    n_cnt       PLS_INTEGER;
    n_max_jobs  PLS_INTEGER;
    v_cmd       VARCHAR2(1000 BYTE);
    v_job_name  VARCHAR2(30);
    v_signal    cnf_data_flows.signal%TYPE;
    
  BEGIN
    xl.open_log(p_data_flow_cd, 'Main process of the "'||p_data_flow_cd||'" Data Flow', TRUE);
    
    -- Marking the start of the Data Flow:
    UPDATE cnf_data_flows
    SET last_proc_id = xl.get_current_proc_id, heartbeat_dt = SYSDATE, signal = 'START'
    WHERE data_flow_cd = p_data_flow_cd
    RETURNING max_num_of_jobs INTO n_max_jobs;
    
    n_cnt := SQL%ROWCOUNT;
    COMMIT;
    
    IF n_cnt = 0 THEN
      Raise_Application_Error(-20000, 'Wrong Data Flow: "'||p_data_flow_cd||'"');
    END IF;
    
    FOR r IN
    (
      SELECT rl.* 
      FROM cnf_data_flow_steps rl
      WHERE data_flow_cd = p_data_flow_cd
      ORDER BY set_num, num
    )
    LOOP
      xl.begin_action('STEP #'||r.set_num||'.'||r.num||'; OPERATION: '||r.operation||', TGT: '||r.tgt||', AS_JOB='||r.as_job);
      
      SELECT signal INTO v_signal
      FROM cnf_data_flows
      WHERE data_flow_cd = p_data_flow_cd;
      
      IF v_signal = 'STOP' THEN
        Raise_Application_Error(-20000, '"STOP" signal received');
      END IF;
      
      IF r.operation = 'WAIT' THEN
        FOR w IN
        (
          SELECT VALUE(t) pattern
          FROM TABLE(split_string(r.tgt)) t
        )
        LOOP
          wait_for_jobs(p_data_flow_cd||'_'||w.pattern, 1);
        END LOOP;
        
      ELSE
        v_cmd := 'BEGIN '|| 
        CASE r.operation
         WHEN 'PROCEDURE' THEN 
          r.tgt||'('''||p_data_flow_cd||''');'
         WHEN 'DELETE' THEN
         'etl.delete_data(p_tgt=>'''||r.tgt||''', p_src=>q''['||r.src||
         ']'', p_whr=>q''['||r.whr||']'', p_hint=>'''||r.hint||''', p_commit_at=>'||r.commit_at||');'
         ELSE -- r.operation IN ('INSERT','APPEND','MERGE' 'REPLACE','EQUALIZE'):
         'etl.add_data(p_operation=>'''||r.operation||''', p_tgt=>'''||r.tgt||
         ''', p_src=>q''['||r.src||']'', p_uk_col_list=>'''||r.uk_col_list||
         ''',p_whr=>q''['||r.whr||']'', p_hint=>'''||r.hint||
         ''', p_errtab=>'''||r.err||''', p_commit_at=>'||r.commit_at||');'
        END || ' END;';
        
        IF r.as_job = 'N' THEN
          xl.begin_action('Executing command', v_cmd, FALSE);
          EXECUTE IMMEDIATE v_cmd;
          xl.end_action;
        ELSE
          v_job_name := p_data_flow_cd||'_'||r.set_num||'_'||r.num;
          
          IF n_max_jobs > 0 THEN
            wait_for_jobs(p_data_flow_cd||'%', n_max_jobs);
          END IF;
          
          start_job(v_job_name, v_cmd);
        END IF;
      END IF;
      
      xl.end_action;
    END LOOP;
    
    wait_for_jobs(p_data_flow_cd||'%', 1);
    
    gather_error_stats(p_data_flow_cd, xl.g_proc_id);
    
    xl.close_log('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    ROLLBACK;
    xl.close_log(SQLERRM, TRUE);
    RAISE;
  END;
  
  
  -- Procedure to stop Data Flow
  PROCEDURE stop_data_flow(p_data_flow_cd IN VARCHAR) IS
    d_upd_dt   DATE;
    d_end_dt   DATE;
    n_cnt      PLS_INTEGER;
    n_pid      dbg_process_logs.proc_id%TYPE;
  BEGIN
    xl.open_log($$plsql_unit||'.STOP_DATA_FLOW', 'Stopping Data Flow "'||p_data_flow_cd||'"', TRUE);
    
    UPDATE cnf_data_flows SET signal = 'STOP' WHERE data_flow_cd = p_data_flow_cd;
    n_cnt := SQL%ROWCOUNT;
    COMMIT;
    
    IF n_cnt = 0 THEN
      Raise_Application_Error(-20000, 'Wrong data flow: '||p_data_flow_cd);
    END IF;
    
    LOOP
      SELECT df.heartbeat_dt, pl.proc_id, pl.end_time
      INTO d_upd_dt, n_pid, d_end_dt
      FROM cnf_data_flows df
      LEFT JOIN dbg_process_logs pl ON pl.proc_id = df.last_proc_id
      WHERE df.data_flow_cd = p_data_flow_cd;
      
      EXIT WHEN d_end_dt IS NOT NULL;
    
      IF n_pid IS NULL THEN
        Raise_Application_Error(-20000, 'Process Log is missing!');
      END IF;
      
      IF d_upd_dt < (SYSDATE - (2*MAX_SLEEP_SECONDS+1)/86400) THEN
        Raise_Application_Error(-20000, '"'||p_data_flow_cd||'" process seems to be dead.');
      END IF;
      
      DBMS_LOCK.SLEEP(MAX_SLEEP_SECONDS);
    END LOOP;
    
    xl.close_log('Successfylly completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.close_log(SQLERRM, TRUE);
    RAISE;
  END;
  
  
  -- Procedure GATHER_ERROR_STATS collects the counts of errors that happened in the given Data Flow.
  PROCEDURE gather_error_stats
  (
    p_data_flow_cd  IN VARCHAR2,
    p_proc_id       IN NUMBER DEFAULT NULL -- if not specified then the latest run of the Data Flow is used
  ) IS
    n_proc_id  cnf_data_flows.last_proc_id%TYPE;
  BEGIN
    xl.open_log($$plsql_unit||'.GATHER_ERROR_STATS', 'Gathering error statistics for the DRF Data Flow "'||p_proc_id||'"', TRUE);
    
    IF p_proc_id IS NOT NULL THEN
      n_proc_id := p_proc_id;
    ELSE
      xl.begin_action('Finding the last last_proc_id', 'Started', FALSE);
      SELECT last_proc_id INTO n_proc_id
      FROM cnf_data_flows WHERE data_flow_cd = p_data_flow_cd;
      xl.end_action('Found: '||n_proc_id);
    END IF;
    
    FOR r IN
    (
      SELECT DISTINCT err 
      FROM cnf_data_flow_steps
      WHERE data_flow_cd = p_data_flow_cd
      AND err IS NOT NULL
    )
    LOOP
      etl.add_data
      (
        p_operation => 'MERGE',
        p_tgt => 'LOG_DRF_ERRORS',
        p_src => 'SELECT
            '''||p_data_flow_cd||''' data_flow_cd,
            pl.proc_id,
            '''||r.err||''' AS err_table,
            err.ora_err_mesg$ AS err_message,
            COUNT(1) err_cnt
          FROM dbg_process_log pl
          JOIN '||r.err||' err ON err.ora_err_tag$ = pl.proc_id
          WHERE pl.name LIKE '''||p_data_flow_cd||'%'' AND pl.proc_id >= '||n_proc_id||'
          GROUP BY pl.proc_id, err.ora_err_mesg$',
        p_commit_at => -1
      );
    END LOOP;
    
    xl.close_log('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.close_log(SQLERRM, TRUE);
    RAISE;
  END gather_error_stats;
END;
/