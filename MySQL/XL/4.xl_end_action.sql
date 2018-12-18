DELIMITER //

CREATE OR REPLACE PROCEDURE xl_end_action(IN p_comment VARCHAR(21000))
BEGIN
  DECLARE r_log_stack   ROW(log_level TINYINT UNSIGNED, action VARCHAR(255), tstamp DATETIME(6), debug BOOLEAN);
  DECLARE dt_now        DATETIME(6);
  DECLARE n_ms          INT UNSIGNED;
  
  IF @g_proc_id IS NOT NULL THEN
    SET dt_now = NOW(6);

    SELECT * INTO r_log_stack FROM tmp_log_stack WHERE log_level = @g_log_level;
    
    SET n_ms = TIMESTAMPDIFF(MICROSECOND, r_log_stack.tstamp, dt_now);
    UPDATE tmp_action_stats SET cnt = cnt+1, microseconds = microseconds + n_ms, last_start_dt = NULL
    WHERE action = r_log_stack.action;
    
    CALL xl_write_log(r_log_stack.action, p_comment, r_log_stack.debug, dt_now);
    
    SET @g_log_level = @g_log_level-1; -- go up by the log stack
  END IF;
END;
//

DELIMITER ;
GRANT EXECUTE ON PROCEDURE xl_end_action TO everybody;
