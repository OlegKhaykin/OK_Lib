UPDATE dbg_process_logs set result = 'Cancelled', end_time = systimestamp
where end_time is null
and proc_id < 82
;
commit;

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
    extract(day from diff) days, extract(hour from diff) hours, extract(minute from diff) minutes, extract(second from diff) seconds
  from
  ( 
    select l.*, nvl(end_time, systimestamp) - start_time diff 
    from dbg_process_logs l
--    where name = 'PREPARE_DSRIP_REPORT_TR016' 
  )
)
order by proc_id desc;

select * from dbg_log_data
where proc_id IN (110)
--and action like 'Adding data to%'
--and comment_txt not like 'Operation%'
order by tstamp;

select proc_id, action, cnt, seconds 
from dbg_performance_data 
where proc_id = 104
order by seconds desc;
