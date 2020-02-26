SELECT output FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1877720674,  6, 179580, 179783)); 

select
  distinct
--  s.begin_interval_time,
  ss.plan_hash_value
from dba_hist_snapshot s
join dba_hist_sqlstat ss on ss.snap_id = s.snap_id and ss.instance_number = s.instance_number
  and ss.sql_id = '0wd2dktqjsmhh'
--where s.begin_interval_time >= timestamp '2019-12-06 00:00:00' and s.end_interval_time < timestamp '2019-12-06 23:59:59'
--order by s.begin_interval_time
;

select --+ parallel(32)
  * 
from dba_hist_sqltext
--where sql_id = '0wd2dktqjsmhh'
where sql_text like '%SELECT /*+ ordered */%CAREENGINEMEMBERPROCESSSTATUS%'
;

select * from 
where  = '1693424455'

select * from dba_hist_sql_bind_metadata;

select * from dba_hist_sqlstat
where sql_id = '6vn5jzmctkbkw'
order by snap_id desc
;

select * from dba_hist_stat_name
where stat_name like '%econd%' or stat_name like '%time%'
;

select * from dba_hist_snapshot where instance_number=3 order by begin_interval_time desc; 

--==============================================
select * from dba_hist_toplevelcall_name;

