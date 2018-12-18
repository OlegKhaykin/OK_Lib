DELIMITER //

CREATE OR REPLACE PROCEDURE xl_write_log(IN p_action VARCHAR(255), IN p_comment VARCHAR(21000), IN p_persist BOOLEAN, IN p_tstamp DATETIME(6))
BEGIN
  IF p_persist THEN
    INSERT INTO dbg_log_data VALUES(@g_proc_id, p_tstamp, @g_log_level, p_action, p_comment);
  ELSE
    SET @g_log_idx = MOD(@g_log_idx, 100) + 1;
    INSERT INTO tmp_log_data VALUES(@g_log_idx, p_tstamp, @g_log_level, p_action, p_comment)
    ON DUPLICATE KEY UPDATE tstamp=p_tstamp, log_level=@g_log_level, action=p_action, comment_txt=p_comment;
  END IF;
END;
//

DELIMITER ;
