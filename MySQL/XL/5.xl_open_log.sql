DROP PROCEDURE IF EXISTS xl_open_log;

DELIMITER //

CREATE PROCEDURE xl_open_log(IN p_name VARCHAR(255), IN p_comment VARCHAR(1000), IN p_debug BOOLEAN)
BEGIN
  IF @g_proc_id IS NULL THEN
    SET @g_proc_id  = NEXTVAL(seq_dbg_process_logs);
    SET @b_debug = p_debug;
    SET @g_call_level = 0;
    SET @g_log_level = 0;
    SET @g_log_idx = 0;
   
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_action_stats
    (
      action          VARCHAR(255) NOT NULL,
      last_start_dt   DATETIME(6),
      cnt             INT UNSIGNED,
      microseconds    INT,
      CONSTRAINT pk_tmp_action_stats PRIMARY KEY(action)
    ) ENGINE=MEMORY;

    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_call_stack
    (
      call_level      TINYINT UNSIGNED NOT NULL,
      module	      VARCHAR(255) NOT NULL,
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
      idx             SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
      tstamp          DATETIME(6) NOT NULL,
      log_level       NUMERIC(2) NOT NULL,
      action          VARCHAR(255) NOT NULL,
      comment_txt     VARCHAR(21000)
    ) ENGINE=MEMORY;

    INSERT INTO dbg_process_logs(proc_id, name, comment_txt) VALUES(@g_proc_id, p_name, p_comment);
  END IF;

  SET @g_call_level = @g_call_level+1;
  INSERT INTO tmp_call_stack VALUES(@g_call_level, p_name, @g_log_level);

  CALL xl_begin_action(p_name, p_comment, @b_debug);
END;
//

DELIMITER ;

GRANT EXECUTE ON PROCEDURE xl_open_log TO everybody;
