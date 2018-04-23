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
  s.audsid, s.sid, s.username, s.osuser,
  lo.opname, lo.target, lo.units, lo.sofar, lo.totalwork, 
  lo.elapsed_seconds, lo.time_remaining 
FROM 
  sess s,
  v$session_longops lo
WHERE (s.status = 'ACTIVE' OR s.sid IN (SELECT parallel_sess_no FROM sess WHERE status = 'ACTIVE'))
--where s.sid=77
AND lo.sid = s.sid AND lo.serial# = s.serial#
AND lo.sofar < lo.totalwork
--ORDER BY s.audsid, username, program;
