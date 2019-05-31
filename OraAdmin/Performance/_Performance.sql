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
    join gv$sqltext sqlt on sqlt.inst_id = s.inst_id and sqlt.sql_id = s.sql_id and sqlt.piece = 0
    where s.status = 'ACTIVE'
--    and s.audsid <> sys_context('userenv','sessionid')
--    and s.osuser = 'khayole'
  ),
  longops as
  (
    select
      s.audsid, s.sql_text,
      lo.*
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
select * from sess  order by audsid;
--select * from waits order by audsid;
--select * from longops order by audsid;
--select * from stats order by audsid;
select * from events order by audsid;
select * from hist where rnk=1 order by audsid;

-- ===========================  SQL execution statistics  ==============================
-- For each SQL statement currently in SGA:
select * from v$sqlarea;
-- For each plan:
select * from v$sqlarea_plan_hash;
-- For each step of the plan:
select * from v$sql_plan_statistics;
-- For each child cursor:
select * from v$sql; 
-- For each execution (SQL_ID + SQL_EXEC_START or SQL_EXEC_ID):
select * v$sql_monitor;

--================================= Execution plans ====================================
-- For the last EXPLAIN PLAN:
select plan_table_output from table(dbms_xplan.display);

-- For the cursor that is still in SGA:
select * from table
(
  dbms_xplan.display_cursor
  (
    sql_id = '...', 
    --cursor_child_no => 0,
    format=ALL
  )
);

-- Plan captured in AWR:
select * from table(dbms_xplan.dosplay_awr('...'));
