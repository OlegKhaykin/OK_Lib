-- Locks:
SELECT
  s. inst_id, s.sid, s.serial#,
  s.osuser, s.username, s.machine, s.program,
  o.owner ||'.'|| o.object_name locked_object,
  Decode(lo.locked_mode, 0,'None', 1,'Null', 2,'Row-S', 3,'Row-X', 4,'Share', 5,'S/Row-X', 6,'Exclusive') as lock_mode
FROM v$locked_object              lo
JOIN dba_objects                  o
  ON lo.object_id = o.object_id
JOIN gv$session                   s
  ON s.sid = lo.session_id AND s.inst_id = lo.inst_id;

-- Blocking locks:
SELECT
  o.owner||'.'||o.object_name||' ('||o.object_type|| ')' object,
  hs.username blocker, hs.sid blocking_sess, hs.program blocking_program, hs.machine blocking_machine, 
  hl.type lock_type, 
  decode(hl.lmode,0,'None',1,'Null',2,'R-SS',3,'R-SX',4,'Shar',5,'SRX',6,'Ex', to_char(hl.lmode)) mode_held,
  ws.username waiter, ws.sid waiting_sess, ws.program waiting_program, ws.machine waiting_machine ,
  decode(wl.request,0,'None',1,'Null',2,'R-SS',3,'R-SX',4,'Shar',5,'SRX',6,'Ex',to_char(wl.request)) mode_requested,
  REPLACE(REPLACE(sql.sql_text,'  ',' '),'"','') waiting_sql
from
  v$lock hl,
  v$lock wl,
  v$session hs,
  v$session ws,
  all_objects o,
  v$sql sql
where hl.lmode not in (0,1) 
and wl.type = hl.type and wl.id1 = hl.id1 and wl.id2 = hl.id2 and wl.request <> 0 
and hs.sid = hl.sid 
and ws.sid = wl.sid
and o.object_id(+) = ws.row_wait_obj#
and sql.sql_id = ws.sql_id;
