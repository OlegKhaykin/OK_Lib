select
  s.sid, s.username, s.program,
  sa.cpu_time, sa.elapsed_time, sa.sql_text
from v$session s, v$sqlarea sa
where s.username is not null
and sa.hash_value = s.sql_hash_value and sa.address = s.sql_address
order by cpu_time desc

