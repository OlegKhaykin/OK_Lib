set sql_mode=ORACLE;

DELIMITER //

CREATE OR REPLACE PROCEDURE prc_multiply(p_debug IN BIT(1)) AS
BEGIN
  xl.open_log('MULTY', 'Multiplication', CASE p_debug WHEN 1 THEN TRUE ELSE FALSE END);
  FOR n IN 1..1 LOOP
    xl.begin_action('Calculating row #'||n, 'Started', TRUE);
    FOR m IN 1..1 LOOP
      xl.begin_action(n||'*'||m||'=', '?', NULL);
      xl.end_action(n*m);
    END LOOP;
--  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'User-defined exception!';
    xl.end_action('Done');
  END LOOP;
  xl.close_log('Successfully completed', FALSE);
EXCEPTION
 WHEN OTHERS THEN
  SET @errmsg = SQLERRM;
  ROLLBACK;
  xl.close_log(@errmsg, TRUE);
  RESIGNAL;
END;
//

DELIMITER ;
