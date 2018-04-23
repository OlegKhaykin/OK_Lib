select 'alter '||decode(object_type,'PACKAGE BODY','PACKAGE','TYPE BODY', 'TYPE',object_type)||' '
||owner||'."'||decode(object_type,'JAVA DATA',dbms_java.LONGNAME(object_name),'JAVA CLASS',
dbms_java.LONGNAME(object_name),'JAVA RESOURCE',dbms_java.LONGNAME(object_name),object_name)||'" compile '
||decode(object_type,'PACKAGE BODY','BODY ;','TYPE BODY','BODY;',' ;') from dba_objects where status <>'VALID'
/
