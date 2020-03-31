SELECT output FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1877720674,  6, 179580, 179783)); 

-- Search SQL by text:
select --+ parallel(32)
  * 
from dba_hist_sqltext
--where sql_id = '0wd2dktqjsmhh'
where regexp_like(sql_text, '^\s*insert\s+into\s+CAREPROVIDERNAME_ETL', 'i')
;

-- Look for the SQL executions:
SELECT sq.con_dbid, sn.snap_id, sn.begin_interval_time, sn.end_interval_time, s.instance_number, sq.sql_id, u.username, s.executions_total, s.executions_delta 
FROM dba_hist_sqltext             sq
JOIN  dba_hist_sqlstat            s
  ON s.sql_id = sq.sql_id
 AND s.con_dbid = sq.con_dbid
JOIN dba_users                    u
  ON u.user_id = s.parsing_user_id
JOIN dba_hist_snapshot            sn
  ON sn.instance_number = s.instance_number
 AND sn.snap_id = s.snap_id
--where sq.sql_id = '2u4x0j94cwsas'
WHERE regexp_like(sql_text, '^\s*insert\s+into\s+CAREPROVIDERNAME_ETL', 'i')
ORDER BY sn.begin_interval_time DESC;

select * from dba_hist_stat_name

where stat_name like '%econd%' or stat_name like '%time%';

select * from dba_hist_snapshot where instance_number=3 order by begin_interval_time desc; 

--==============================================
select * from dba_hist_toplevelcall_name;
