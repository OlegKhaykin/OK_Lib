select s.sid, t.name, s.value
from gv$sesstat s
join v$statname t
  on s.statistic# = t.statistic#
where t.name='undo change vector size'
order by s.value desc;

/*
select 
  rs.segment_name,
  sum(s.bytes)/1024/1024 mbytes 
from
  dba_rollback_segs rs,
  dba_segments s
where s.segment_name = rs.segment_name
group by rs.segment_name
order by mbytes desc;

drop table test_oleg;
create table test_oleg as select * from v$rollstat where rownum<1;
alter table test_oleg add tstamp date default sysdate;
*/

spool undo_stats.log
set echo on

insert into test_oleg select rs.*, sysdate from v$rollstat rs;

INSERT INTO lcdbuser.t_tmp_trade_gmi_h 
SELECT DISTINCT
  to_date('20070103','YYYYMMDD'), a.*, 51568
FROM lcdbuser.t_temp_trade_gmi a 
WHERE record_number IN 
(
  SELECT record_number 
  FROM emdb.t_em_process_log_exception
  WHERE process_log_id = 51568
  AND reload_table_name = 'T_TEMP_TRADE_GMI'
);

insert into test_oleg select rs.*, sysdate from v$rollstat rs;

select
  t1.usn, 
  t1.tstamp start_time, 
  t2.tstamp end_time, 
  t2.writes - t1.writes bytes_added  
from
  v$session s,
  v$transaction tr,
  test_oleg t1,
  test_oleg t2
where s.audsid = sys_context('USERENV','SESSIONID')
and tr.addr = s.taddr
and t1.usn = tr.xidusn and t1.tstamp = (select min(tstamp) from test_oleg)
and t2.usn = tr.xidusn and t2.tstamp = (select max(tstamp) from test_oleg);

rollback;
truncate table test_oleg;

spool off
