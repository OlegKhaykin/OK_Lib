--create or replace view v_sessions as 
select  
  inst_id, sid, serial#, username, program, machine, status,
  days,
  case when days>0 then 0 else mod(hours,24) end hours,
  case when days>0 or hours>0 then 0 else mod(minutes,60) end minutes,
  case when days>0 or hours>0 or minutes>0 then 0 else mod(seconds,60) end seconds,
  last_call_et,
  concat_v2_set(cursor(
    select replace(replace(sql_text,'  ',' '),'"','') 
    from gv$sqltext 
    where inst_id = s.inst_id
    and address = s.sql_address
    and hash_value = s.sql_hash_value 
    order by piece
  ), ' ') sql_text
from
(
  select
    inst_id, sid, serial#, pid, spid, 
    username, program, machine, status, last_call_et, 
    sql_address, sql_hash_value,
    trunc(duration) days,
    trunc(duration*24) hours,
    trunc(duration*24*60) minutes,
    trunc(duration*24*60*60) seconds
  from
  (
    select
      s.inst_id, s.sid, s.serial#, s.username, s.program, s.machine, s.status, s.last_call_et,
      p.pid, p.spid,
      s.sql_address, s.sql_hash_value,
      (sysdate-logon_time) duration
    from gv$session s
    join gv$process p on p.inst_id = s.inst_id and p.addr = s.paddr
    where sid = 3670
  )
) s;

grant select on v_sessions to dba;
create public synonym v_sessions for v_sessions;
