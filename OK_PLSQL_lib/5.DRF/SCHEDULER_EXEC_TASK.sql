prompt Creating Oracle Scheduler program "EXEC_TASK"

begin
  for r in
    (
    select program_name
    from user_scheduler_programs
    where program_name = 'EXEC_TASK'
  )
  loop
    dbms_scheduler.drop_program('EXEC_TASK');
  end loop;
end;
/

whenever sqlerror exit 1 
BEGIN
  DBMS_SCHEDULER.CREATE_PROGRAM
  (
    program_name => 'EXEC_TASK',
    program_type => 'STORED_PROCEDURE',
    program_action => 'DRF.EXEC_TASK',
    number_of_arguments => 2,
    enabled => FALSE,
    comments => 'None'
  );

  DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT
  (
    program_name => 'EXEC_TASK',
    argument_position => 1,
    argument_name => 'P_JOB_NAME',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT
  (
    program_name => 'EXEC_TASK',
    argument_position => 2,
    argument_name => 'P_TASK',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  DBMS_SCHEDULER.ENABLE('EXEC_TASK');
end;
/
