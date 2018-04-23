select 'alter '||decode(object_type,'PACKAGE BODY','PACKAGE','TYPE BODY', 'TYPE',object_type)||' '
||owner||'."'||object_name||'" compile '
||decode(object_type,'PACKAGE BODY','BODY ;','TYPE BODY','BODY;',' ;') 
from dba_objects where owner='SYS' and status <> 'VALID'
and object_type not like 'JAVA%' and object_type <> 'SYNONYM';
