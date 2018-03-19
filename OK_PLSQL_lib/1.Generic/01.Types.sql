begin
  for r in
  (
    select owner, type_name from all_types
    where owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
    and type_name in 
    (
      'OBJ_NAME_VALUE','TAB_NAME_VALUES',
      'OBJ_COLUMN_INFO','TAB_COLUMN_INFO',
      'TAB_V256'
    )
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
CREATE TYPE tab_name_values AS TABLE OF obj_name_value;
/
CREATE OR REPLACE PUBLIC SYNONYM obj_name_value FOR obj_name_value;
GRANT EXECUTE ON obj_name_value TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM tab_name_values FOR tab_name_values;
GRANT EXECUTE ON tab_name_values TO PUBLIC;

CREATE TYPE obj_column_info AS OBJECT
(
  OWNER           VARCHAR2(30),
  TABLE_NAME      VARCHAR2(30),
  COLUMN_ID       INTEGER,
  COLUMN_NAME     VARCHAR2(30),
  DATA_TYPE       VARCHAR2(106),
  DATA_LENGTH     INTEGER,
  DATA_PRECISION  INTEGER,
  DATA_SCALE      INTEGER,
  NULLABLE        CHAR(1),
  PK              CHAR(1)
);
/

CREATE TYPE tab_column_info AS TABLE OF obj_column_info;
/

CREATE OR REPLACE PUBLIC SYNONYM obj_column_info FOR obj_column_info;
GRANT EXECUTE ON obj_column_info TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM tab_column_info FOR tab_column_info;
GRANT EXECUTE ON tab_column_info TO PUBLIC;

CREATE OR REPLACE TYPE tab_v256 AS TABLE OF VARCHAR2(256);
/

CREATE OR REPLACE PUBLIC SYNONYM tab_v256 FOR tab_v256;
GRANT EXECUTE ON tab_v256 TO PUBLIC;
