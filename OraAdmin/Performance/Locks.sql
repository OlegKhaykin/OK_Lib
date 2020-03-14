-- Locks:
SELECT
  s. inst_id, s.sid, s.serial#,
  s.osuser, s.username, s.machine, s.program,
  o.owner ||'.'|| o.object_name locked_object,
  Decode(lo.locked_mode, 0,'None', 1,'Null', 2,'Row-S', 3,'Row-X', 4,'Share', 5,'S/Row-X', 6,'Exclusive') as lock_mode
FROM gv$locked_object             lo
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
  REPLACE(REPLACE(sq.sql_text,'  ',' '),'"','') waiting_sql
from gv$lock                                                      hl -- holding lock
join gv$lock                                                      wl -- waiting_lock 
  on wl.type = hl.type
 and wl.id1 = hl.id1 and wl.id2 = hl.id2 and wl.request <> 0 
join gv$session                                                   hs -- holding session
  on hs.inst_id = hl.inst_id and hs.sid = hl.sid 
join gv$session                                                   ws -- waiting session
  on ws.inst_id = hs.inst_id and ws.sid = wl.sid 
left join all_objects                                             o
  on o.object_id = ws.row_wait_obj#
left join gv$sql                                                  sq
  on sq.inst_id = ws.inst_id and sq.sql_id = ws.sql_id
where hl.lmode not in (0,1);

select s.*, lck.* 
from dba_objects                        o
join gv$lock                            lck 
  on lck.id1 = o.object_id 
join gv$session                         s
  on s.inst_id = lck.inst_id and s.sid = lck.sid 
where o.owner = 'AHMADMIN' and o.object_name = 'TMP_PSA_PROCESS_RESULTS';