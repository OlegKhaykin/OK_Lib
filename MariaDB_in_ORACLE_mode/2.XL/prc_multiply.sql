set sql_mode=ORACLE;

DELIMITER //

CREATE OR REPLACE PROCEDURE prc_multiply(p_test_num IN SMALLINT, p_debug IN BIT(1)) AS
BEGIN
  xl.open_log
  (
    'Test-'||p_test_num||' '||CASE p_debug WHEN 1 THEN 'with' ELSE 'no' END || ' debug', 
    'Multiplication', 
    CASE p_debug WHEN 1 THEN TRUE ELSE FALSE END
  );

  FOR n IN 1..9 LOOP
    xl.begin_action('Calculating row #'||n, 'Started', TRUE);
    FOR m IN 1..9 LOOP
      xl.begin_action(n||'*'||m||'=', '?', NULL);
      xl.end_action(n*m);
    END LOOP;
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
