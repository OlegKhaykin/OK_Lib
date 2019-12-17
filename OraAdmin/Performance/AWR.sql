SELECT output FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1877720674,  6, 179580, 179783)); 

select
  s.*
from dba_hist_snapshot s
join dba_hist_sqlstat ss on ss.snap_id = s.snap_id and ss.instance_number = s.instance_number
  and sql_id = '0wd2dktqjsmhh'
--where s.begin_interval_time >= timestamp '2019-12-13 16:00:00' and s.end_interval_time < timestamp '2019-12-13 23:00:00'
order by s.begin_interval_time desc;

select --+ parallel(32)
  * 
from dba_hist_sqltext
where sql_text like '%SELECT /*+ ordered */%CAREENGINEMEMBERPROCESSSTATUS%'
;


select * from dba_hist_sql_bind_metadata;

select * from dba_hist_sqlstat;

select * from dba_hist_stat_name
where stat_name like '%econd%' or stat_name like '%time%'
;

select * from dba_hist_snapshot where instance_number=3 order by begin_interval_time desc; 

--==============================================
select * from dba_hist_toplevelcall_name;

