prompt Creating Oracle Scheduler program "DATA_REFRESH_SLAVE"

whenever sqlerror continue 
exec dbms_scheduler.drop_program('DATA_REFRESH_SLAVE');

whenever sqlerror exit 1 
begin
  dbms_scheduler.create_program
  (
    program_name => '&fusion_schema..DATA_REFRESH_SLAVE',
    program_type => 'STORED_PROCEDURE',
    program_action => 'PKG_PME_DATA_REFRESH.SLAVE',
    number_of_arguments => 4,
    enabled => FALSE,
    comments => 'None'
  );

  dbms_scheduler.define_program_argument
  (
    program_name => '&fusion_schema..DATA_REFRESH_SLAVE',
    argument_position => 1,
    argument_name => 'P_ASSIGNMENT_TABLE',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  dbms_scheduler.define_program_argument
  (
    program_name => '&fusion_schema..DATA_REFRESH_SLAVE',
    argument_position => 2,
    argument_name => 'P_REFRESH_TYPE',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  dbms_scheduler.define_program_argument
  (
    program_name => '&fusion_schema..DATA_REFRESH_SLAVE',
    argument_position => 3,
    argument_name => 'P_SLAVE_NUMBER',
    argument_type => 'NUMBER',
    out_argument => FALSE
  );

  dbms_scheduler.define_program_argument
  (
    program_name => '&fusion_schema..DATA_REFRESH_SLAVE',
    argument_position => 4,
    argument_name => 'P_KEY_COLUMN',
    argument_type => 'VARCHAR2',
    out_argument => FALSE
  );

  dbms_scheduler.enable('&fusion_schema..DATA_REFRESH_SLAVE');
end;
/

prompt Creating procedure START_DATA_REFRESH_SLAVE for Oracle 11g database

CREATE OR REPLACE PROCEDURE start_data_refresh_slave
(
  p_assignment_table IN VARCHAR2,
  p_refresh_type     IN VARCHAR2,
  p_slave_number     IN PLS_INTEGER,
  p_key_column       IN VARCHAR2
) IS
  jname  VARCHAR2(30);
BEGIN
 -- 11/26/2012, OK: created original version;
  jname := p_refresh_type||'_S'||p_slave_number; 
  
  DBMS_SCHEDULER.CREATE_JOB 
  (
    job_name => jname,
    job_style => 'LIGHTWEIGHT',
    program_name => 'DATA_REFRESH_SLAVE',
    enabled => FALSE
  );

  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
  (
    job_name => jname,
    argument_name => 'P_ASSIGNMENT_TABLE',
    argument_value => p_assignment_table
  );

  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
  (
    job_name => jname,
    argument_name => 'P_REFRESH_TYPE',
    argument_value => p_refresh_type
  );

  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
  (
    job_name => jname,
    argument_name => 'P_SLAVE_NUMBER',
    argument_value => p_slave_number
  );

  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE
  (
    job_name => jname,
    argument_name => 'P_KEY_COLUMN',
    argument_value => p_key_column
  );

  DBMS_SCHEDULER.ENABLE(jname);
end;
/
