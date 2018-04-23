--select * from
--(
SELECT
  NVL(s.username, 'Internal') username,
  substr(s.program, 1, instr(s.program,' ')) Program,
  p.spid,
  s.sid||','||s.serial# kill,
  NVL(s.terminal, 'None') terminal,
  s.machine,
  TO_CHAR(s.logon_time, 'yy/mm/dd hh24:mi:ss') time,
  s.status status,
  REPLACE(REPLACE(sql_text,'  ',' '),'"','') text
FROM
  v$session s,
  v$sqltext sq,
  v$process p
WHERE sid = 19 --and s.type = 'USER' AND s.username != 'SYSTEM' AND s.status = 'ACTIVE'
AND sq.address = s.sql_address AND sq.hash_value = s.sql_hash_value
AND s.paddr = p.addr
ORDER BY 4, 1, 2, sq.piece
--)
--where upper(text) like 'SELECT O.CHILDID%'

select * from v$session_wait where sid=19

select * from v$session where sid=147