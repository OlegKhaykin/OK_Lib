drop table my_sql_stat purge;

CREATE TABLE my_sql_stat
(
  dtime         date,
  inst_id       NUMBER(1),
  sql_id        varchar2(13),
  sql_text      clob,
  executions    number,
  elapsed_time  number,
  rnum          number(34)
);

drop table my_sess_stat purge;

create table my_sess_stat as
select sysdate as dtime, inst_id, audsid, sid, serial#, username, sql_id, osuser, program
FROM gv$session s
where status = 'ACTIVE'
and sql_id is not null
and rownum < 1;

declare 
  dt date;
begin
--  loop
    dt := sysdate;
    
    insert into my_sql_stat 
    select dt, inst_id, sql_id, sql_fulltext, executions, elapsed_time, null
    from gv$sqlarea ;
    
    insert into my_sess_stat
    select dt, inst_id, audsid, sid, serial#, username, sql_id, osuser, program
    FROM gv$session s
    where status = 'ACTIVE'
    and sql_id is not null;
    
    commit;
--  end loop;
end;
/

merge into my_sql_stat t
using (select dtime, inst_id, sql_id, rank() over(partition by inst_id, sql_id order by dtime) rnk from my_sql_stat) q
on (t.dtime = q.dtime and t.inst_id = q.inst_id and t.sql_id = q.sql_id)
when matched then update set t.rnum = q.rnk;

commit;

--==============================================================================
select /*+ parallel(32) */ count(1) cnt from my_sql_stat;
 
select --+ parallel(32)
  sqs.inst_id, sqs.sql_id, sqs.start_dt, sqs.executions, sqs.elapsed_time, to_char(substr(sqs.sql_text, 1, 2000)) sql_text, sqs.rn
from
(
  select
    msqs.*,
    min(dtime) over(partition by sql_id) start_dt,
    row_number() over(partition by sql_id order by elapsed_time desc) rn
  from my_sql_stat msqs
) sqs
where sqs.rn = 1
order by sqs.elapsed_time desc;

select s1.inst_id, s1.sql_id, s2.executions-s1.executions execs, s2.elapsed_time - s1.elapsed_time etime, s1.sql_text
from my_sql_stat s1
join my_sql_stat s2 on s2.inst_id = s1.inst_id and s2.sql_id = s1.sql_id and s2.rnum = s1.rnum+1
where s1.rnum = 1
order by etime desc;
