DELIMITER //

CREATE OR REPLACE PROCEDURE xl_write_log(IN p_action VARCHAR(255), IN p_comment VARCHAR(21000), IN p_persist BOOLEAN, IN p_tstamp DATETIME(6))
BEGIN
  IF p_persist THEN
    INSERT INTO dbg_log_data VALUES(@g_proc_id, p_tstamp, @g_log_level, p_action, p_comment);
  ELSE
    SET @action = p_action;
    SET @comment = p_comment;
    SET @now = p_tstamp;
    SET @g_log_idx = @g_log_idx + 1;

    IF @g_log_idx <= 100 THEN
      EXECUTE write_tmp_log_1 USING @g_log_idx, @now, @g_log_level, @action, @comment;
      IF @g_log_idx = 100 THEN
        EXECUTE truncate_tmp_log_2;
      END IF;
    ELSE
      EXECUTE write_tmp_log_2 USING @g_log_idx, @now, @g_log_level, @action, @comment;
      IF @g_log_idx = 200 THEN
        EXECUTE truncate_tmp_log_1;
        SET @g_log_idx = 0;
      END IF;
    END IF;
  END IF;
END;
//

DELIMITER ;
