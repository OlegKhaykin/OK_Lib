-- Test: 27-Sep-2019
DECLARE
  my_task_name VARCHAR2(30);
  my_sqltext CLOB;
BEGIN
  xl.open_logg('TST_TUNE','Tuning SQL',TRUE);
  
  my_sqltext := '
  SELECT *
  FROM tst_ok_people
  WHERE last_name = :name';

  my_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK
  (
    sql_text => my_sqltext,
    bind_list => sql_binds(anydata.ConvertDate('10-MAY-2009')),
    user_name => 'OK',
    scope => 'COMPREHENSIVE',
    time_limit => 600,
    task_name => 'CREDIT CARD CAPTURE',
    description => 'Task to tune the Credit Card capture procedure'
  );
  
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/

BEGIN
  DBMS_SQLTUNE.EXECUTE_TUNING_TASK(task_name => 'CREDIT CARD CAPTURE');
END;
/
