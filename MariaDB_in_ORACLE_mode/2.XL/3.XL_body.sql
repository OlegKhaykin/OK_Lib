SET sql_mode=ORACLE;

DELIMITER //

CREATE OR REPLACE PACKAGE BODY xl AS
  g_proc_id     SMALLINT UNSIGNED;
  g_log_level   SMALLINT UNSIGNED;
  g_call_level  SMALLINT UNSIGNED;
  g_log_idx     SMALLINT UNSIGNED;
  b_debug       BOOLEAN;

  PROCEDURE open_log(p_name IN VARCHAR, p_comment IN VARCHAR, p_debug BOOLEAN) IS
  BEGIN
    IF g_proc_id IS NULL THEN
      g_proc_id    := seq_dbg_process_logs.NEXTVAL;
      b_debug      := p_debug;
      g_call_level := 0;
      g_log_level  := 0;
      g_log_idx    := 0;
     
      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_action_stats
      (
        action          VARCHAR(255) NOT NULL,
        tstamp          DATETIME(6),
        cnt             INT UNSIGNED,
        microseconds    INT,
        CONSTRAINT pk_tmp_action_stats PRIMARY KEY(action)
      ) ENGINE=MEMORY;

      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_call_stack
      (
        call_level      TINYINT UNSIGNED NOT NULL,
        module	        VARCHAR(255) NOT NULL,
        log_level       TINYINT UNSIGNED NOT NULL,
        CONSTRAINT pk_tmp_call_stack PRIMARY KEY(call_level)
      ) ENGINE=MEMORY;

      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_log_stack
      (
        log_level       TINYINT UNSIGNED NOT NULL,
        action	        VARCHAR(255) NOT NULL,
        tstamp          DATETIME(6) NOT NULL,
        debug		        BOOLEAN NOT NULL,
        CONSTRAINT pk_tmp_log_stack PRIMARY KEY(log_level)
      ) ENGINE=MEMORY;

      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_log_data
      (
        idx             SMALLINT UNSIGNED NOT NULL,
        tstamp          DATETIME(6) NOT NULL,
        log_level       NUMERIC(2) NOT NULL,
        action          VARCHAR(255) NOT NULL,
        comment_txt     VARCHAR(21000),
        CONSTRAINT pk_tmp_log_data PRIMARY KEY(idx)
      ) ENGINE=MEMORY;

      INSERT INTO dbg_process_logs(proc_id, name, comment_txt)
      VALUES(g_proc_id, p_name, p_comment);
    END IF;

    g_call_level := g_call_level+1;
    INSERT INTO tmp_call_stack VALUES(g_call_level, p_name, g_log_level);

    begin_action(p_name, p_comment, b_debug);
  END;


  PROCEDURE write_log
  (
    p_action    IN VARCHAR, 
    p_comment   IN VARCHAR, 
    p_persist   IN BOOLEAN,
    p_tstamp    IN DATETIME(6)
  ) IS
  BEGIN
    IF p_persist THEN
      INSERT INTO dbg_log_data VALUES(g_proc_id, p_tstamp, g_log_level, p_action, p_comment);
    ELSE
      g_log_idx := MOD(g_log_idx, 1000) + 1;
      INSERT INTO tmp_log_data VALUES(g_log_idx, p_tstamp, g_log_level, p_action, p_comment)
      ON DUPLICATE KEY UPDATE tstamp=p_tstamp, log_level=g_log_level, action=p_action, comment_txt=p_comment;
    END IF;
  END;

    
  PROCEDURE begin_action
  (
    p_action    IN VARCHAR, 
    p_comment   IN VARCHAR, 
    p_debug     IN BOOLEAN
  ) IS
    b_persist   BOOLEAN;
    dt_recent   DATETIME(6);
    dt_now      DATETIME(6);
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      dt_now := NOW(6);
      b_persist := CASE WHEN p_debug OR p_debug IS NULL AND b_debug THEN TRUE ELSE FALSE END;
      
      SELECT MAX(tstamp) INTO dt_recent FROM tmp_action_stats WHERE action = p_action;
      IF dt_recent IS NOT NULL THEN 
        SET @errmsg = CONCAT('Action "', p_action, '" is already running. You cannot start it again! This is a bug!');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @errmsg;
      END IF;
      
      INSERT INTO tmp_action_stats VALUES(p_action, dt_now, 0, 0)
      ON DUPLICATE KEY UPDATE tstamp = dt_now;
      
      g_log_level := g_log_level+1;
      INSERT INTO tmp_log_stack VALUES(g_log_level, p_action, dt_now, b_persist)
      ON DUPLICATE KEY UPDATE action = p_action, debug = b_persist;
      
      write_log(p_action, p_comment, b_persist, dt_now);
    END IF;
  END;
  
  
  PROCEDURE end_action(p_comment IN VARCHAR2) IS
    r_log_stack   tmp_log_stack%ROWTYPE;
    dt_now        DATETIME(6);
    n_ms          INT;
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      dt_now := NOW(6);
      
      SELECT * INTO r_log_stack FROM tmp_log_stack WHERE log_level = g_log_level;
      
      n_ms := TIMESTAMPDIFF(MICROSECOND, r_log_stack.tstamp, dt_now);
      UPDATE tmp_action_stats SET cnt = cnt+1, microseconds = microseconds + n_ms, tstamp = NULL
      WHERE action = r_log_stack.action;
      
      write_log(r_log_stack.action, p_comment, r_log_stack.debug, dt_now);
      
      g_log_level := g_log_level-1; -- go up by the log stack
    END IF;
  END;
  
  
  PROCEDURE close_log(p_result IN VARCHAR2, p_dump IN BOOLEAN) IS
    r_call_stack  tmp_call_stack%ROWTYPE;
  BEGIN
    IF g_proc_id IS NOT NULL THEN -- if logging has been started in this session:
      SELECT * INTO r_call_stack FROM tmp_call_stack WHERE call_level = g_call_level;
      
      WHILE g_log_level > r_call_stack.log_level LOOP
        end_action(p_result);
      END LOOP;
       
      g_call_level := g_call_level-1; -- go up the call stack:
      
      IF g_call_level = 0 THEN -- if this is the end of the main process
        IF p_dump THEN -- if request received to dump debugging information accumulated in memory (usually from an exception handler):
          -- Save log data accumulated in memory:
          INSERT INTO dbg_log_data 
          SELECT g_proc_id, tstamp, log_level, action, comment_txt
          FROM tmp_log_data;
        END IF;
      
        -- Save performance statistics accumulated in memory:
        INSERT INTO dbg_performance_data(proc_id, action, cnt, seconds)
        SELECT g_proc_id, action, cnt, microseconds/1000000
        FROM tmp_action_stats;
        
        DROP TABLE IF EXISTS tmp_action_stats;
        DROP TABLE IF EXISTS tmp_log_stack;
        DROP TABLE IF EXISTS tmp_call_stack;
        DROP TABLE IF EXISTS tmp_log_data;
        
        UPDATE dbg_process_logs SET end_time = NOW(6), result = p_result
        WHERE proc_id = g_proc_id;
       
        g_proc_id := NULL;
      END IF;
    END IF;
  END;
  
  
  FUNCTION get_current_proc_id RETURN SMALLINT UNSIGNED IS
  BEGIN
    RETURN g_proc_id;
  END;
END;
//

DELIMITER ;
