select s1.sql_id, s2.executions-s1.executions execs, s2.elapsed_time - s1.elapsed_time etime, s1.sql_text
from my_stat s1
join my_stat s2 on s2.sql_id = s1.sql_id and s2.rnum = 45 
where s1.rnum = 1
order by execs desc;