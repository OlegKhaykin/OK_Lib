begin
  for r in
  (
    select owner, type_name from all_types
    where owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
    and type_name in ('OBJ_NAME_VALUE','TAB_NAME_VALUES','TAB_V256')
    order by type_name desc
  )
  loop
    execute immediate 'drop type '||r.owner||'.'||r.type_name;
  end loop;
end;
/

CREATE TYPE obj_name_value AS OBJECT
(
  name  VARCHAR2(100),
  value VARCHAR2(256)
);
/
GRANT EXECUTE ON obj_name_value TO PUBLIC;

CREATE TYPE tab_name_values AS TABLE OF obj_name_value;
/
GRANT EXECUTE ON tab_name_values TO PUBLIC;

CREATE OR REPLACE TYPE tab_v256 AS TABLE OF VARCHAR2(256);
/
GRANT EXECUTE ON tab_v256 TO PUBLIC;

CREATE OR REPLACE TYPE obj_column_definition AS OBJECT
(
  column_name   VARCHAR2(128),
  data_type     VARCHAR2(30),
  nullable      VARCHAR2(10)
);
/
GRANT EXECUTE ON obj_column_definition TO PUBLIC;

CREATE OR REPLACE TYPE tab_column_definitions AS TABLE OF obj_column_definition;
/
GRANT EXECUTE ON tab_column_definitions TO PUBLIC;
