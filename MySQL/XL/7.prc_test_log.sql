DELIMITER //

CREATE OR REPLACE PROCEDURE prc_test_log(IN p_test_num SMALLINT, IN p_debug BIT(1))
BEGIN
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
  
  FOR n IN 1..9 DO
    CALL xl_begin_action(CONCAT('Calculating row #', n), 'Started', TRUE);

    FOR m IN 1..9 DO
      CALL xl_begin_action('Calculating', CONCAT(n,'*',m,'='), NULL);
      CALL xl_end_action(n*m);
    END FOR;

    CALL xl_end_action('Done');
  END FOR;

  #SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Test error';

  CALL xl_close_log('Successfully completed', FALSE);
END;
//

DELIMITER ;
