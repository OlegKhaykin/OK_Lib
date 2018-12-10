DELIMITER //

CREATE OR REPLACE PROCEDURE xl_begin_action(IN p_action VARCHAR(255), IN p_comment VARCHAR(21000), IN p_debug BOOLEAN) 
BEGIN
  DECLARE b_persist BOOLEAN;
  DECLARE dt_recent DATETIME(6);
  DECLARE dt_now    DATETIME(6);

  IF @g_proc_id IS NOT NULL THEN
    SET dt_now = NOW(6);
    SET b_persist = CASE WHEN p_debug OR p_debug IS NULL AND @b_debug THEN TRUE ELSE FALSE END;
    
    SELECT MAX(tstamp) INTO dt_recent FROM tmp_action_stats WHERE action = p_action;
    IF dt_recent IS NOT NULL THEN
      SET @errmsg = CONCAT('Action "', p_action, '" is already running. You cannot start it again! This is a bug!');
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @errmsg;
    END IF;
    
    INSERT INTO tmp_action_stats VALUES(p_action, dt_now, 0, 0)
    ON DUPLICATE KEY UPDATE tstamp = dt_now;
    
    SET @g_log_level = @g_log_level+1;
    INSERT INTO tmp_log_stack VALUES(@g_log_level, p_action, dt_now, b_persist)
    ON DUPLICATE KEY UPDATE action = p_action, debug = b_persist;
    
    CALL xl_write_log(p_action, p_comment, b_persist, dt_now);
  END IF;
END;
//

DELIMITER ;
GRANT EXECUTE ON PROCEDURE xl_begin_action TO everybody;
