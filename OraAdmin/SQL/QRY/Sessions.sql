WITH sess AS
(
  SELECT 
    s.inst_id, s.sid, s.serial#,
    s.username, s.osuser, s.program, s.machine, p.pid, p.spid, status,
    s.audsid, 
    DECODE(ownerid, 2147483644, NULL, TRUNC(ownerid/65536)) parallel_coord_id,
    DECODE(ownerid, 2147483644, NULL, MOD(ownerid, 65536)) parallel_sess_no 
  FROM gv$session s
  JOIN gv$process p ON p.inst_id = s.inst_id AND p.addr = s.paddr
  WHERE s.username IS NOT NULL
  AND s.program like 'sqlplus%'
)
SELECT
  'alter system disconnect session '''||s.sid||','||s.serial#||',@'||s.inst_id||''' immediate;' cmd,
  s.* FROM sess s
--where spid=24848
--WHERE (status = 'ACTIVE' OR sid IN (SELECT parallel_sess_no FROM sess WHERE status = 'ACTIVE'))
--where username = 'JCREW_CUSTOM' and sid=917
--and osuser = 'odi' 
ORDER BY program-- audsid, parallel_sess_no
;

alter system disconnect session '581,20037,@1' immediate;