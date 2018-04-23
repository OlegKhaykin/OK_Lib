select 
  s.sid, s.machine, s.osuser, s.username, s.status,
  REPLACE(REPLACE(sq.sql_text,'  ',' '),'"','') text
from
  v$process p,
  v$session s,
  v$sqltext sq
where p.spid = 24848
and s.paddr = p.addr
AND sq.address(+) = s.sql_address AND sq.hash_value(+) = s.sql_hash_value
order by piece;
