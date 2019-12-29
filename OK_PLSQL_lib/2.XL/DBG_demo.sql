create or replace type obj_test as object
(
  n_key     NUMBER(2),
  v_value   VARCHAR2(30)
);
/
  
create or replace type tab_test is table of obj_test;
/

create or replace function get_tab_test_val(p_data IN sys.AnyData) RETURN tab_test IS
  n       PLS_INTEGER;
  t_cnf   tab_test;
BEGIN
  n := p_data.GetCollection(t_cnf);
  RETURN t_cnf;
END;
/


declare
  t_tst tab_test;
begin
  xl.open_log
  (
    p_name => 'TEST',
    p_comment => 'This is a test',
    p_log_level => 1 -- the default log level is 0
  ); 
  
  xl.begin_action('Action-1');
    xl.begin_action('Action-2');
    
    t_tst := tab_test(obj_test(1,'One'), obj_test(2, 'Two')); 
    
    xl.write_suppl_data('PT_PROGRAMCONFIGURATION_IN', sys.AnyData.ConvertCollection(t_tst));
    
    raise_application_error(-20000, 'Something bad has happened');
    
    xl.end_action;
  xl.end_action('Completed');
  
  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log
  (
    p_result => SQLERRM,
    p_dump => TRUE
  );
  
  raise; -- Never "swallow" exception, let them rise!
end;
/

select
  proc_id, name,
  comment_txt, 
  result,
  start_time,
  case when days > 1 then days||' days ' when days > 0 then '1 day ' end ||
  case when days > 0 or hours > 0 then hours || ' hr ' end ||
  case when days > 0 or hours > 0 or minutes > 0 then minutes || ' min ' end ||
  round(seconds)|| ' sec' time_spent
from
(
  select
    proc_id, name, comment_txt, result,
    start_time, end_time,
    extract(day from diff) days, 
    extract(hour from diff) hours, 
    extract(minute from diff) minutes, 
    extract(second from diff) seconds
  from
  ( 
    select l.*, nvl(end_time, systimestamp) - start_time diff 
    from dbg_process_logs l
--    where name = 'PREPARE_DSRIP_REPORT_TR016' 
  )
)
order by proc_id desc;

select
  proc_id, tstamp, log_depth, action, to_char(substr(comment_txt,1,255)) result
--  , comment_txt
from dbg_log_data
where proc_id IN (1, 2, 3)
--and action like 'Adding data to%'
--and comment_txt not like 'Operation%'
order by tstamp desc;

select proc_id, action, cnt, seconds 
from dbg_performance_data 
where proc_id in (1, 2)
order by proc_id, seconds desc;

select
  dsd.proc_id, dsd.name, dsd.tstamp, dsd.value.GetTypeName(),
  t.*
from dbg_supplemental_data dsd
cross join table(get_tab_test_val(dsd.value)) t;
