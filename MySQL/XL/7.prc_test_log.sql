DROP PROCEDURE IF EXISTS prc_test_log;

DELIMITER //

CREATE PROCEDURE prc_test_log(IN p_test_num SMALLINT, IN p_debug BIT(1))
BEGIN
  DECLARE n SMALLINT UNSIGNED;
  DECLARE m SMALLINT UNSIGNED;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @errmsg = MESSAGE_TEXT;
    ROLLBACK;
    CALL xl_close_log(@errmsg, TRUE);
    RESIGNAL;
  END;
  
  CALL xl_open_log
  (
    CONCAT('Test-', p_test_num, ' ', CASE p_debug WHEN 1 THEN 'with' ELSE 'no' END, ' debug'), 
    'Logging test',
    CASE p_debug WHEN 1 THEN TRUE ELSE FALSE END
  );
  
  SET n = 1;
  WHILE n <= 10 DO
    CALL xl_begin_action(CONCAT('Calculating row #', n), 'Started', TRUE);
    
    SET m = 1;
    WHILE m <= 100 DO
      CALL xl_begin_action('Calculating', CONCAT(n,'*',m,'='), NULL);
      CALL xl_end_action(n*m);
    END WHILE;

    CALL xl_end_action('Done');
  END WHILE;

  #SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Test error';

  CALL xl_close_log('Successfully completed', FALSE);
END;
//

DELIMITER ;
