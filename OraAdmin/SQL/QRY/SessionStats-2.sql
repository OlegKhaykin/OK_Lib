select s.sid, s.machine, s.program, s.username, sn.name, ss.value, sql_address, sql_hash_value
from v$session s, v$sesstat ss, v$statname sn
where ss.sid = s.sid and sn.statistic# = ss.statistic#
and sn.name = 'CPU used by this session' order by value desc
