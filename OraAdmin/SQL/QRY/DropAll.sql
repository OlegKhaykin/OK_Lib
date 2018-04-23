select 'drop '||object_type||' '||object_name||decode(object_type, 'TABLE', ' cascade constraints;', ';')
from user_objects
where object_type in ('VIEW','MATERIALIZED VIEW','TABLE','SEQUENCE','PROCEDURE','FUNCTION','PACKAGE','JAVA SOURCE')
