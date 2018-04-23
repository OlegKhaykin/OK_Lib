WITH sess AS
(
  SELECT 
    s.audsid, s.sid, s.serial#,
    s.username, s.osuser, s.program, s.machine, p.pid, p.spid, status,
    DECODE(ownerid, 2147483644, NULL, TRUNC(ownerid/65536)) parallel_coord_id,
    DECODE(ownerid, 2147483644, NULL, MOD(ownerid, 65536)) parallel_sess_no 
  FROM v$session s, v$process p
  WHERE s.username IS NOT NULL AND p.addr = s.paddr
)
SELECT * FROM sess
--where spid=24848
--WHERE (status = 'ACTIVE' OR sid IN (SELECT parallel_sess_no FROM sess WHERE status = 'ACTIVE'))
--where username = 'JCREW_CUSTOM' and sid=917
--and osuser = 'odi' 
ORDER BY program-- audsid, parallel_sess_no
;

alter system flush shared_pool;

select count(1) from v$process; 

select username, 
--machine, 
--program, 
status, count(1) cnt
from v$session
group by username, 
--machine, 
--program, 
status
order by cnt desc;