set head off
spool upd.sql

SELECT
  'ALTER ' ||
  Decode(object_type,'PACKAGE BODY','PACKAGE','TYPE BODY', 'TYPE',object_type) || ' ' ||
  owner || '."' ||
  Decode
  (
    object_type,
    'JAVA DATA', dbms_java.LONGNAME(object_name),
    'JAVA CLASS', dbms_java.LONGNAME(object_name),
    'JAVA RESOURCE', dbms_java.LONGNAME(object_name),
    object_name
  ) || '" compile ' ||
  Decode(object_type, 'PACKAGE BODY', 'BODY;', 'TYPE BODY', 'BODY;', ';')
FROM dba_objects
WHERE status <> 'VALID'
ORDER BY owner, object_type;

spool off
set head on
