col username for a12
col machine for a20
col program for a20
col locked_object for a30
col lock_mode for a10

SELECT
  s.sid,
  s.username,
  s.machine,
  s.program,
  o.owner ||'.'|| o.object_name locked_object,
  Decode
  (
    lo.locked_mode,
    0,'None',
    1,'Null',
    2,'Row-S',
    3,'Row-X',
    4,'Share',
    5,'S/Row-X',
    6,'Exclusive'
  ) as lock_mode
FROM
  v$session s, v$locked_object lo, dba_objects o
WHERE
  s.sid = lo.session_id AND lo.object_id = o.object_id;
