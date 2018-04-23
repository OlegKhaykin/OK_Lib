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
