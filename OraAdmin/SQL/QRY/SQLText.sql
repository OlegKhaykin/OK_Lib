select * from
(
  SELECT
    NVL(l.username, 'Internal') username,
    substr(l.program, 1, instr(l.program,' ')) Program,
    p.spid,
    l.sid||','||l.serial# kill,
    NVL(l.terminal, 'None') terminal,
    l.machine,
    TO_CHAR(l.logon_time, 'yy/mm/dd hh24:mi:ss') time,
    l.status status,
    REPLACE(REPLACE(sql_text,'  ',' '),'"','') text
  FROM
    v$session l,
    v$sqltext sq,
    v$process p
  WHERE l.type = 'USER'
  AND sq.address = l.sql_address
  AND l.paddr = p.addr
  AND l.username != 'SYSTEM'
  AND l.status = 'ACTIVE'
  ORDER BY 4, 1, 2, sq.piece
)
where upper(text) like '%JC_WEB_INVENTORY%'
