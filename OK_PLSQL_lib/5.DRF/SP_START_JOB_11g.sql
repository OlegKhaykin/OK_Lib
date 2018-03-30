prompt Creating Oracle Scheduler program "EXEC_CMD"

define fusion_schema=FUSION

begin
  for r in
  (
    select program_name
    from all_scheduler_programs
    where owner = '&fusion_schema'
    and program_name = 'EXEC_CMD'
  )
  loop
    dbms_scheduler.drop_program('&fusion_schema..EXEC_CMD');
  end loop;
end;
/

whenever sqlerror exit 1 
begin
  dbms_scheduler.create_program
  (
    program_name => '&fusion_schema..EXEC_CMD',
    program_type => 'STORED_PROCEDURE',
    program_action => 'SP_EXEC_CMD',
    number_of_arguments => 1,
    enabled => FALSE,
    comments => 'None'
  );

  dbms_scheduler.define_program_argument
  (
    program_name => '&fusion_schema..EXEC_CMD',
    argument_position => 1,
    argument_name => 'P_CMD',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  dbms_scheduler.enable('&fusion_schema..EXEC_CMD');
end;
/

prompt Creating procedure SP_START_JOB for Oracle 11g database

CREATE OR REPLACE PROCEDURE sp_start_job(p_job_name IN VARCHAR2, p_task IN VARCHAR2) AS
BEGIN
 -- 5-Nov-2012, OK: created original version;
  DBMS_SCHEDULER.CREATE_JOB 
  (
    job_name => p_job_name,
    job_style => 'LIGHTWEIGHT',
    program_name => 'EXEC_CMD',
    enabled => FALSE
  );

  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
  (
    job_name => p_job_name,
    argument_name => 'P_CMD',
    argument_value => 'begin '||p_task||' end;'
  );

 DBMS_SCHEDULER.ENABLE(p_job_name);
end;
/
CREATE OR REPLACE PUBLIC SYNONYM sp_start_job FOR sp_start_job;
