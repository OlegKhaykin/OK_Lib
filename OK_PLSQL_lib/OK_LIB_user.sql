create user ok_lib identified by m default tablespace users temporary tablespace temp;

grant connect, resource, create view, create synonym, select_catalog_role, create public synonym to ok_lib;
grant execute on dbms_lock to ok_lib;