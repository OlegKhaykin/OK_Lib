prompt Creating view V_DB_PROCESSES

create or replace view v_db_processes as
select s.sid, s.serial#, s.username, s.status, s.program, s.machine, s.logon_time, s.last_call_et, s.paddr, p.pid, p.spid 
from 
  v$session s,
  v$process p
where p.addr(+) = s.paddr;

grant select on v_db_processes to dba;
create public synonym v_db_processes for v_db_processes;
