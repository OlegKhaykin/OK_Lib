CREATE TABLE my_stat
(
  SQL_ID        VARCHAR2(13),
  EXECUTIONS    NUMBER,
  ELAPSED_TIME  NUMBER
  SQL_TEXT      VARCHAR2(1000),
  DTSTAMP       DATE,
  rnum          number(34)
);

alter table my_stat add primary key (sql_id, dtstamp);
alter table my_stat add unique (rnum, sql_id);

select sql_id, count(1) from v$sqlarea
group by sql_id having count(1)>1;

begin
  loop
    insert into my_stat 
    select sql_id, executions, sysdate, sql_text, elapsed_time, null from v$sqlarea;
    commit;
    
    dbms_lock.sleep(60);
  end loop;
end;
  
merge into my_stat t
using (select sql_id, dtstamp, rank() over(partition by sql_id order by dtstamp) rnk from my_stat) q
on (t.sql_id = q.sql_id and t.dtstamp= q.dtstamp)
when matched then update set t.rnum = q.rnk;

commit;

select s1.sql_id, s2.executions-s1.executions execs, s2.elapsed_time - s1.elapsed_time etime, s1.sql_text
from my_stat s1
join my_stat s2 on s2.sql_id = s1.sql_id and s2.rnum = 45 
where s1.rnum = 1
order by execs desc;
