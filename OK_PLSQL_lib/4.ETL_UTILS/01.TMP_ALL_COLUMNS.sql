prompt Creating table TMP_ALL_COLUMNS

begin
  for r in
  (
    select object_name
    from all_objects
    where owner = sys_context('userenv', 'current_schema')
    and object_type = 'TABLE'
    and object_name = 'TMP_ALL_COLUMNS'
  )
  loop
    execute immediate 'drop table '||r.object_name;
  end loop;
end;
/
  
CREATE GLOBAL TEMPORARY TABLE tmp_all_columns
(
  side         CHAR(3) CONSTRAINT chk_col_side CHECK(side IN ('SRC','TGT')),
  owner        VARCHAR2(30) NOT NULL,
  table_name   VARCHAR2(30) NOT NULL,
  column_id    NUMBER       NOT NULL,
  column_name  VARCHAR2(30) NOT NULL,
  data_type    VARCHAR2(30) NOT NULL,
  uk           CHAR(1)      NOT NULL,
  nullable     CHAR(1)      NOT NULL,
  CONSTRAINT pk_tmp_all_columns PRIMARY KEY(side, column_name)
)
ON COMMIT DELETE ROWS;
