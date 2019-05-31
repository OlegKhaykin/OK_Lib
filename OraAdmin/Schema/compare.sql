PROMPT To run this script you should be connected to the ORADEV database as DBA
SET PAGESIZE 100
COL owner FOR a20
COL object_name FOR a30

spool compare

SELECT
  u.owner, u.object_type,
  Decode
  (
    u.object_type,
    'JAVA DATA', dbms_java.LONGNAME(u.object_name),
    'JAVA CLASS', dbms_java.LONGNAME(u.object_name),
    'JAVA RESOURCE', dbms_java.LONGNAME(u.object_name),
    u.object_name
  ) object_name,
  NVL(d.status, 'Absent') dev_status,
  NVL(p.status, 'Absent') prod_status
FROM
(
  SELECT owner, object_name, object_type
  FROM dba_objects
  WHERE owner IN
  (
   'CENTRAL','COMM','GPS','LOAD','PROJ','STATEFARM','OMWB_EMULATION',
   'REL1','TIME1','EST1','XDK','SCOTT'
  )
  AND object_type IN 
  (
    'TABLE','INDEX','TRIGGER','VIEW','PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION',
    'TYPE','TYPE BODY','SEQUENCE','JAVA DATA','JAVA CLASS','JAVA RESOURCE'
  )
  UNION
  SELECT owner, object_name, object_type
  FROM dba_objects@production
  WHERE owner IN 
  (
   'CENTRAL','COMM','GPS','LOAD','PROJ','STATEFARM','OMWB_EMULATION',
   'REL1','TIME1','EST1','XDK','SCOTT'
  )
  AND object_type IN 
  (
    'TABLE','INDEX','TRIGGER','VIEW','PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION',
    'TYPE','TYPE BODY','SEQUENCE','JAVA DATA','JAVA CLASS','JAVA RESOURCE'
  )
) u,
  dba_objects d, dba_objects@production p
WHERE d.owner(+) = u.owner
AND d.object_name(+) = u.object_name
AND d.object_type(+) = u.object_type
AND p.owner(+) = u.owner
AND p.object_name(+) = u.object_name
AND p.object_type(+) = u.object_type
AND (p.status <> 'VALID' OR d.status <> 'VALID' OR p.status IS NULL OR d.status IS NULL)
ORDER BY u.owner, u.object_type, u.object_name;

spool off
