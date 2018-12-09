select * from information_schema.routines;

DELETE FROM dbg_process_logs;
DELETE FROM dbg_log_data;
DELETE FROM dbg_performance_data;

select * from dbg_process_logs order by proc_id desc;

select q.*, timestampdiff(MICROSECOND, first_start_ts, last_end_ts) total_mcs
from
(
  select sq.name, count(1) cnt, min(sq.start_time) first_start_ts, max(sq.end_time) last_end_ts, round(avg(sq.mcs)) avg_mcs
  from
  (
    select l.*, timestampdiff(MICROSECOND, start_time, end_time) mcs
    from dbg_process_logs l
  ) sq
  group by name
) q
order by name;

select count(1) cnt from dbg_log_data;

select * from dbg_log_data
where proc_id = 200
order by proc_id desc, tstamp;

select * from dbg_performance_data
where proc_id = 1
order by seconds desc;

select action, SUM(cnt) cnt, AVG(seconds) sec
from dbg_performance_data pf
#where proc_id = 657
group by pf.action
order by sec desc;

select * from tst_ok where num>1;