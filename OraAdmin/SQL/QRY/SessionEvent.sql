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
  s.sid, event, total_waits waits, total_timeouts timeouts,
  time_waited total_time, average_wait avg
FROM 
  sess s,
  v$session_event e
WHERE s.status = 'ACTIVE' 
AND e.sid = s.sid
ORDER BY sid, time_waited DESC;