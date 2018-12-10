select * from information_schema.routines;

DELETE FROM dbg_process_logs;
DELETE FROM dbg_log_data;
DELETE FROM dbg_performance_data;

commit;

select * from dbg_process_logs order by proc_id desc;

select q.*, timestampdiff(MICROSECOND, first_start_ts, last_end_ts) total_mcs
from
(
  select
    sq.name, max(sq.proc_id) last_proc_id, count(1) cnt, sum(sq.dtl_cnt) log_cnt,
    min(sq.start_time) first_start_ts, max(sq.end_time) last_end_ts,
    round(avg(sq.mcs)) avg_mcs
  from
  (
    select
      l.name, l.proc_id, l.start_time, l.end_time,
      timestampdiff(MICROSECOND, l.start_time, l.end_time) mcs,
      count(1) dtl_cnt
    from dbg_process_logs l
    join dbg_log_data d on d.proc_id = l.proc_id
    group by l.name, l.proc_id, l.start_time, l.end_time
  ) sq
  group by name
) q
order by name;

select * from dbg_log_data
where proc_id = 300
order by proc_id desc, tstamp;

select * from dbg_performance_data
where proc_id = 1
order by seconds desc;

select action, SUM(cnt) cnt, AVG(seconds) sec
from dbg_performance_data pf
group by pf.action
order by sec desc;
