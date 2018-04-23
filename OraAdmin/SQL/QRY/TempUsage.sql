set lines 120
col name for a60

var blksize number
set feed off
exec select block_size into :blksize from dba_tablespaces where contents = 'TEMPORARY' and rownum < 2;
set feed on

select a.name, b.name, b.bytes/1024/1024 Mb 
from v$tablespace a, v$tempfile b
where a.ts# = b.TS#
/

col username for a15
col sid for 999999
col serial# for 9999999
col program for a30
col osuser for a10
col tablespace for a12
col sze for 9999999.999 head "Size (Mb)"
break on report
compute sum of sze on report

select u.username, sid, serial#, s.program, s.osuser, s.status, TABLESPACE, CONTENTS, BLOCKS*:blksize/1024/1024 sze
--, BLOCKS
from v$session s, v$sort_usage u
where s.SADDR = u.SESSION_ADDR
/
