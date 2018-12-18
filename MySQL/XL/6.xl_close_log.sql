DELIMITER //

CREATE OR REPLACE PROCEDURE xl_close_log(IN p_result VARCHAR(2048), IN p_dump BOOLEAN)
BEGIN
  DECLARE r_call_stack ROW(call_level TINYINT UNSIGNED, module VARCHAR(255), log_level TINYINT UNSIGNED);

  IF @g_proc_id IS NOT NULL THEN -- if logging has been started in this session:
    SELECT * INTO r_call_stack FROM tmp_call_stack WHERE call_level = @g_call_level;
    
    WHILE @g_log_level > r_call_stack.log_level DO
      CALL xl_end_action(p_result);
    END WHILE;
     
    SET @g_call_level = @g_call_level-1; -- go up the call stack:
    
    IF @g_call_level = 0 THEN -- if this is the end of the main process
      IF p_dump THEN -- if request received to dump debugging information accumulated in memory (usually from an exception handler):
        -- Save log data accumulated in memory:
        INSERT INTO dbg_log_data
        SELECT @g_proc_id, tstamp, log_level, action, comment_txt 
        FROM tmp_log_data;
      END IF;         
    
      -- Save performance statistics accumulated in memory:
      INSERT INTO dbg_performance_data(proc_id, action, cnt, seconds)
      SELECT @g_proc_id, action, cnt, microseconds/1000000
      FROM tmp_action_stats;
      
      DROP TABLE IF EXISTS tmp_action_stats;
      DROP TABLE IF EXISTS tmp_log_stack;
      DROP TABLE IF EXISTS tmp_call_stack;
      DROP TABLE IF EXISTS tmp_log_data;
      
      UPDATE dbg_process_logs SET end_time = NOW(6), result = p_result
      WHERE proc_id = @g_proc_id;
     
      SET @g_proc_id = NULL;
    END IF;
  END IF;
END;
//

DELIMITER ;

GRANT EXECUTE ON PROCEDURE xl_close_log TO everybody;