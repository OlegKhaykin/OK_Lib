with
  bsize AS
  (
    select distinct block_size 
    from dba_tablespaces
    where contents = 'TEMPORARY'
  )
select
  s.username, s.sid, s.serial#, s.program, s.osuser, s.status, 
  su.tablespace, su.contents, su.blocks, su.blocks * bs.block_size/1024/1024 MB
from bsize                      bs
cross join gv$sort_usage        su
join gv$session                 s
  on s.saddr = su.session_addr
 and s.inst_id = su.inst_id;
