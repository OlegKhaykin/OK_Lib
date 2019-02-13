DROP PROCEDURE IF EXISTS xl_end_action;

DELIMITER //

CREATE PROCEDURE xl_end_action(IN p_comment VARCHAR(21000))
BEGIN
  DECLARE dt_now        DATETIME(6);
  DECLARE v_action 		VARCHAR(255);
  DECLARE dt_begin	 	DATETIME(6);
  DECLARE b_debug 		BOOLEAN;
  DECLARE n_ms          INT UNSIGNED;
  
  IF @g_proc_id IS NOT NULL THEN
    SELECT NOW(6), action, tstamp, debug
    INTO dt_now, v_action, dt_begin, b_debug 
    FROM tmp_log_stack
    WHERE log_level = @g_log_level;
    
    UPDATE tmp_action_stats SET
      cnt = cnt+1, 
      microseconds = microseconds + TIMESTAMPDIFF(MICROSECOND, dt_begin, dt_now),
      last_start_dt = NULL
    WHERE action = v_action;
    
    CALL xl_write_log(v_action, p_comment, b_debug, dt_now);
    
    SET @g_log_level = @g_log_level-1; -- go up by the log stack
  END IF;
END;
//

DELIMITER ;
GRANT EXECUTE ON PROCEDURE xl_end_action TO everybody;
