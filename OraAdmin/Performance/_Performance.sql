-- Current session state and stats:
with
  sess as
  (
    select
      s.audsid, s.inst_id, s.sid, s.serial#, p.pid, p.spid,
      decode(s.ownerid, 2147483644, null, trunc(s.ownerid/65536)) parallel_coord_id,
      decode(s.ownerid, 2147483644, null, mod(s.ownerid,65536)) parent_sess_sid,
      s.username, s.osuser,
      s.program, s.module, s.action
      , s.status, s.blocking_session,
      s.sql_id, s.sql_child_number, sqlt.sql_text
    from gv$session s
    join gv$process p on p.inst_id = s.inst_id and p.addr = s.paddr
    left join gv$sqltext sqlt on sqlt.inst_id = s.inst_id and sqlt.sql_id = s.sql_id and sqlt.piece = 0
    where 1=1
    and s.status IN ('ACTIVE','KILLED')
    --and s.audsid <> sys_context('userenv','sessionid')
    and s.osuser <> 'oracle'
    --and upper(sql_text) like '%MEMBERPDCSCOREHIST%'
    --and s.username = 'N384433'
    --and s.module = 'SQL*Plus'
    --and s.sid = 499
    --and s.audsid = 618435790
    --and upper(s.program) ='SQLPLUS.EXE'
    --and s.sql_id is not null
  ),
  longops as
  (
    select
      s.audsid, s.sql_text,
      lo.elapsed_seconds, lo.time_remaining, lo.message,
      lo.sql_id, lo.sql_exec_id
    from sess s
    join gv$session_longops lo
      on lo.inst_id = s.inst_id and lo.sid = s.sid and lo.serial# = s.serial#
  ),
  waits as
  (
    select
      --s.*,
      s.audsid, s.inst_id, s.sid,
      w.seq#, w.event, w.wait_class, w.state, w.wait_time_micro/1000000 wait_seconds, w.time_since_last_wait_micro/1000000 seconds_since_last_wait,
      w.p1text, p1, w.p2text, p2, w.p3text, p3
    from sess s
    join gv$session_wait w on w.inst_id = s.inst_id and w.sid = s.sid
  ),
  events as
  (
    select
      s.audsid, s.inst_id, s.sid,
      se.event, se.time_waited_micro/1000000 waited_seconds
    from sess s
    join gv$session_event se on se.inst_id = s.inst_id and se.sid = s.sid
  ),
  stats as
  (
    select
      --s.*,
      s.audsid, s.inst_id, s.sid,
      sn.name, ss.value
    from sess s
    join gv$sesstat ss
      on ss.inst_id = s.inst_id and ss.sid = s.sid and value > 0
    join gv$statname sn
      on sn.inst_id = s.inst_id and sn.statistic# = ss.statistic#
  ),
  hist as
  (
    select
      --s.*,
      s.audsid, s.inst_id, s.sid,
      ash.event,
      rank() over(partition by s.audsid, s.inst_id, s.sid order by sample_time desc) rnk
    from sess s
    join gv$active_session_history ash
      on ash.inst_id = s.inst_id and ash.session_id = s.sid and ash.session_serial# = s.serial#
  )
select * from sess  order by audsid, sid;
select * from waits order by audsid, wait_seconds desc;
select * from waits order by audsid;
select * from longops order by audsid, time_remaining desc, elapsed_seconds desc;
--select * from stats order by audsid;
select * from events order by audsid;
select * from hist where rnk=1 order by audsid;

select * from table(dbms_xplan.display_cursor(sql_id => '9hr22b850678j', format => 'ALL'));

-- ===========================  SQL execution statistics  ==============================
-- For each SQL statement currently in SGA:
select
--  a.address, c.parent_handle
  a.*, c.* 
from gv$sqlarea     a
left join gv$sql_cursor  c
  on c.parent_handle = a.address and c.inst_id = a.inst_id
where 1=1
--and a.parsing_schema_name = 'POC' and a.module = 'SQL*Plus'
--and a.sql_id = 'bz3wgf0189a4h'
and upper(a.sql_text) like '%INSERT%CAREPROVIDERNAME_ETL%'
;

select * from gv$session where sql_id = 'frzbwrc0qwj78';

-- For each plan:
select * from gv$sqlarea_plan_hash;

-- For each step of the plan:
select * from gv$sql_plan_statistics;

-- For each child cursor:
select * from gv$sql_cursor;

-- For each execution (SQL_ID + SQL_EXEC_START or SQL_EXEC_ID):
select * from gv$sql_monitor;

--================================= Execution plans ====================================
-- For the last EXPLAIN PLAN:
explain plan for
SELECT PS.MEMBERID FROM AHMMRNBUSINESSSUPPLIER BS 
JOIN CAREENGINEMEMBERPROCESSSTATUS PS ON PS.MEMBERID = BS.AHMMRNMEMBERID WHERE BS.LASTBUSINESSAHMSUPPLIERID =12906 ;

select plan_table_output from table(dbms_xplan.display);

-- For the cursor that is still in SGA:
select * from table
(
  dbms_xplan.display_cursor
  (
    sql_id =>
      'gghbyydza9mk9',
    cursor_child_no => 0,
    format => 'ALL'
  )
);

-- Plan captured in AWR:
select * from table(dbms_xplan.display_awr('...'));

