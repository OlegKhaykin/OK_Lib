set linesize 132
column owner format a10
column name  format a15
column hsid   format 99999
column wsid   format 99999
column blocker  format a10
column waiter  format a10
column type  format a10
column object  format a40
column mode_held format a10 heading MR
column mode_requested format a10 heading MH
column ltype format a2 heading "LT"
column lmode format 9 heading "M"
column sid   format 99999
column serial   format 99999
column lock_type format a15 heading "Lock Type"
column osuser format a8
column program format a30
column wrowid  format a20

SELECT /*+ RULE */
  B.USERNAME Blocker, A.USERNAME Waiter, H.SID HSID, W.SID WSID,
  W.TYPE  LOCK_TYPE,
  DECODE(H.LMODE,0,'None',1,'Null',2,'R-SS',3,'R-SX',4,'Shar',5,'SRX',6,'Ex', TO_CHAR(H.LMODE)) MODE_HELD,
  DECODE(W.REQUEST,0,'None',1,'Null',2,'R-SS',3,'R-SX',4,'Shar',5,'SRX',6,'Ex',TO_CHAR(W.REQUEST)) MODE_REQUESTED,
  O.OWNER||'.'||O.OBJECT||' ('||O.TYPE|| ')' object,
  W.ID1 LOCK_ID1,W.ID2 LOCK_ID2
FROM
  V$SESSION B,
  V$ACCESS O,
  V$LOCK H,
  V$LOCK W,
  V$SESSION A
WHERE H.LMODE <> 0 AND H.LMODE <> 1 AND W.REQUEST <> 0 AND H.TYPE = W.TYPE
AND H.ID1=W.ID1 AND H.ID2=W.ID2 
AND B.SID=H.SID 
AND W.SID=A.SID 
AND (O.SID=W.SID OR O.SID=H.SID) AND O.OWNER <> 'SYS';
