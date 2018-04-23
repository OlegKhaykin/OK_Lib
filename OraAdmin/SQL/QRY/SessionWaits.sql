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
SELECT
  s.audsid, s.sid, s.username, s.osuser, s.program, s.status,
  w.state, w.event, w.p1text, w.p1, w.p2text, w.p2, w.p3text, p3
FROM 
  sess s,
  v$session_wait w
WHERE (s.status = 'ACTIVE' OR s.sid IN (SELECT parallel_sess_no FROM sess WHERE status = 'ACTIVE'))
--and s.osuser = 'okhaykin'
AND w.sid = s.sid
ORDER BY s.username
