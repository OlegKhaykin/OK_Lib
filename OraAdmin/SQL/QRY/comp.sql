PROMPT To run this script you should be connected to ORADEV database as DBA
SET PAGESIZE 100
SET LINE 200
COL owner FOR a20
COL object_name FOR a30

SELECT
  u.owner, u.object_type, u.object_name,
  NVL(d.status, 'Absent') dev_status,
  NVL(p.status, 'Absent') prod_status
FROM
(
  SELECT owner, object_name, object_type
  FROM dba_objects
  WHERE object_type = 'PACKAGE BODY'
  AND object_name IN
  (
   'DBMS_ASYNCRPC_PUSH','DBMS_DEFER_INTERNAL_SYS','DBMS_DEFER_SYS_PART1',
   'DBMS_IAS_MT_INST','DBMS_REPCAT_MIG_INTERNAL'
  )
  UNION
  SELECT owner, object_name, object_type
  FROM dba_objects@production
  WHERE object_type = 'PACKAGE BODY'
  AND object_name IN
  (
   'DBMS_ASYNCRPC_PUSH','DBMS_DEFER_INTERNAL_SYS','DBMS_DEFER_SYS_PART1',
   'DBMS_IAS_MT_INST','DBMS_REPCAT_MIG_INTERNAL'
  )
) u,
  dba_objects d, dba_objects@production p
WHERE d.owner(+) = u.owner
AND d.object_name(+) = u.object_name
AND d.object_type(+) = u.object_type
AND p.owner(+) = u.owner
AND p.object_name(+) = u.object_name
AND p.object_type(+) = u.object_type
ORDER BY u.owner, u.object_type, u.object_name;
