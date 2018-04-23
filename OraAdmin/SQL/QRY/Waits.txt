create table adm_harmless_waits (event varchar2(64) primary key) organization index;

insert into adm_harmless_waits
select * from table(v2_array(
'lock element cleanup', 
 'pmon timer', 
 'rdbms ipc message',
 'rdbms ipc reply',
 'smon timer', 
 'SQL*Net message from client', 
 'SQL*Net break/reset to client',
 'SQL*Net message to client',
 'SQL*Net more data from client',
 'dispatcher timer',
 'Null event',
 'parallel query dequeue wait',
 'parallel query idle wait - Slaves',
 'pipe get',
 'PL/SQL lock timer',
 'slave wait',
 'virtual circuit status',
 'WMON goes to sleep',
 'jobq slave wait',
 'Queue Monitor Wait',
 'wakeup time manager',
 'PX Idle Wait'
));

commit;

create or replace view vw_system_waits as
select
  event, total_waits, round((time_waited/100),2) time_wait_sec,
  total_timeouts, round((average_wait/100),2) average_wait_sec
from sys.v_$system_event
where event not in (select event from adm_harmless_waits)
and event not like 'DFS%' and event not like 'KXFX%';

create or replace view vw_session_waits as
select
  s.sid, se.event,
  decode (s.username, null, bg.name, s.username) username,
  se.total_waits, round((se.time_waited/100),2) time_wait_sec,
  se.total_timeouts, round((se.average_wait/100),2) average_wait_sec,
  round((se.max_wait/100),2) max_wait_sec
from sys.v_$session s
join sys.v_$session_event se on se.sid = s.sid
left outer join sys.v_$bgprocess bg on bg.paddr = s.paddr
where se.event not in (select event from adm_harmless_waits)
and se.event not like 'DFS%' and se.event not like 'KXFX%';

select
  event,
  total_waits, round(100 * (total_waits/sum_waits),2) pct_waits,
  time_wait_sec, round(100 * (time_wait_sec/greatest(sum_time_waited,1)),2) pct_time_waited, 
  total_timeouts, round(100 * (total_timeouts/greatest(sum_timeouts,1)),2) pct_timeouts, 
  average_wait_sec
from vw_system_waits,
(
  select
    sum(total_waits) sum_waits,
    sum(total_timeouts) sum_timeouts,
    sum(time_wait_sec) sum_time_waited
  from vw_system_waits
) order by 4 desc, 1 asc;

select
  sid, username, event, total_waits, 
  100*round((total_waits/sum_waits),2) pct_of_total_waits,
  time_wait_sec, total_timeouts, average_wait_sec, max_wait_sec
from
  vw_session_waits,
  (
    select sum(total_waits) sum_waits
    from vw_session_waits
  )
order by 6 desc, 1 asc;
