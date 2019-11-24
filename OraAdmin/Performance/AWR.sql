select * from dba_hist_snapshot
order by begin_interval_time desc;

SELECT output FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1877720674,  6, 179580, 179783)); 

select * from dba_hist_sqltext;

select * from dba_hist_sql_bind_metadata;

select * from dba_hist_sqlstat;

select * from dba_hist_stat_name;

select * from dba_hist_snapshot where instance_number=3 order by begin_interval_time desc; 

--==============================================
select * from dba_hist_toplevelcall_name;

