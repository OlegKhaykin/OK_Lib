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
select s.sid, s.serial#, s.parallel_sess_no, s.machine, s.program, s.username, s.osuser, sn.name, st.value
from
  sess s,
  v$sesstat st,
--  v$mystat st,
  v$statname sn
where s.status = 'ACTIVE' 
and st.sid = s.sid and st.value> 0 
and sn.statistic# = st.statistic#
and sn.name IN
(
  'dummy'
  ,'CPU used by this session' 
--  ,'workarea memory allocated'
--  ,'number of auto extends on undo tablespace'
)
--and s.osuser = 'okhaykin'
order by sn.name, st.value desc
