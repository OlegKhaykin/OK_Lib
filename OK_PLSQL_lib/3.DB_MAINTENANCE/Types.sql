CREATE OR REPLACE TYPE obj_column_definition AS OBJECT
(
  column_name VARCHAR2(128),
  data_type   VARCHAR2(30),
  nullable    CHAR(1) 
);
/
GRANT EXECUTE ON obj_column_definition TO PUBLIC;

CREATE OR REPLACE TYPE tab_column_definitions AS TABLE OF obj_column_definition;
/
GRANT EXECUTE ON tab_column_definitions TO PUBLIC;
