CREATE OR REPLACE PACKAGE pkg_db_maintenance AUTHID CURRENT_USER AS
  FUNCTION get_column_definitions(p_column_definitions IN VARCHAR2) RETURN tab_column_definitions PIPELINED;
  
  PROCEDURE add_columns
  (
    p_column_definitions IN VARCHAR2,
    p_table_list         IN VARCHAR2
  );
END;
/
CREATE OR REPLACE SYNONYM adm FOR pkg_db_maintenance;
GRANT EXECUTE ON adm TO PUBLIC;

CREATE OR REPLACE PACKAGE BODY pkg_db_maintenance AS
  PROCEDURE exec_sql(p_sql IN VARCHAR2, p_force IN BOOLEAN DEFAULT FALSE) IS
  BEGIN
    IF LENGTH(p_sql) > 255 THEN
      xl.begin_action('Executing SQL', p_sql);
    ELSE
      xl.begin_action(p_sql, 'Started');
    END IF;
    
    EXECUTE IMMEDIATE p_sql;
    xl.end_action; 
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    IF NOT p_force THEN
      RAISE;
    END IF;
  END;
  
  
  FUNCTION get_column_definitions(p_column_definitions IN VARCHAR2) RETURN tab_column_definitions PIPELINED IS
    
    FUNCTION get_col_dfn(p_col_dfn IN VARCHAR2) RETURN obj_column_definition IS
      ret   obj_column_definition;
    BEGIN
      EXECUTE IMMEDIATE 'BEGIN :ret := obj_column_definition('''||REPLACE(p_col_dfn, ':', ''',''')||'''); end;' USING OUT ret;
      RETURN ret;
    END;
  BEGIN
    FOR r IN
    (
      SELECT COLUMN_VALUE cdef
      FROM TABLE(split_string(p_column_definitions))
    )
    LOOP
      PIPE ROW(get_col_dfn(r.cdef));
    END LOOP;
    
    RETURN;
  END;
  
  PROCEDURE add_columns
  (
    p_column_definitions IN VARCHAR2,
    p_table_list         IN VARCHAR2
  ) IS
    cmd_list TAB_V256;
  BEGIN
    SELECT
      'ALTER TABLE '||tl.COLUMN_VALUE||
      CASE WHEN utc.owner IS NULL THEN ' ADD ' ELSE ' MODIFY ' END ||
      tcd.column_name||' '||tcd.data_type||
      CASE WHEN tcd.nullable <> NVL(utc.nullable, 'NULL') THEN CASE tcd.nullable WHEN 'N' THEN ' NOT NULL' ELSE ' NULL' END END cmd
    BULK COLLECT INTO cmd_list 
    FROM TABLE(pkg_db_maintenance.get_column_definitions(p_column_definitions)) tcd
    CROSS JOIN TABLE(split_string(p_table_list)) tl
    LEFT JOIN v_all_columns utc
      ON utc.owner = SYS_CONTEXT('USERENV','CURRENT_USER')
     AND utc.table_name = UPPER(tl.COLUMN_VALUE)
     AND utc.column_name = UPPER(tcd.column_name)
    WHERE utc.owner IS NULL OR UPPER(tcd.data_type) <> REPLACE(REPLACE(utc.data_type, ' CHAR)', ')'), ' BYTE)', ')') OR utc.nullable <> tcd.nullable;
     
    FOR I in 1..cmd_list.COUNT LOOP
      exec_sql(cmd_list(i));
    END LOOP;
  END;
END;
/
