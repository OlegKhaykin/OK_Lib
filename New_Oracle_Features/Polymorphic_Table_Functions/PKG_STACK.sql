/* Please, use the following sources:
1) PL/SQL Language Reference - Chapter 12 Optimization and Tuning - Paragraph 12.6 - Overview of Polymorphic Table Functions (PTF)
2) PL/SQL Packages and Types Reference - DBMS_TF;
3) 
*/
CREATE OR REPLACE PACKAGE pkg_ptf_stack AS 
  FUNCTION describe(tab  IN OUT dbms_tf.table_t, col dbms_tf.columns_t) RETURN dbms_tf.describe_t; 
 
  PROCEDURE fetch_rows; 
END pkg_ptf_stack;
/

CREATE OR REPLACE PACKAGE BODY pkg_ptf_stack AS 
  FUNCTION describe(tab IN OUT dbms_tf.table_t, col dbms_tf.columns_t) RETURN dbms_tf.describe_t  IS 
  BEGIN 
    FOR i IN 1 .. tab.column.count LOOP 
      FOR j IN 1 .. col.count LOOP 
        IF tab.column(i).description.name = col(j) AND tab.column(i).description.TYPE = dbms_tf.type_number THEN 
          tab.column(i).pass_through := false; 
          tab.column(i).for_read := true; 
        END IF;
      END LOOP;
    END LOOP;
    
    RETURN dbms_tf.describe_t
    ( 
      new_columns => dbms_tf.columns_new_t
      ( 
        1 => dbms_tf.column_metadata_t(name => 'COLUMN_NAME', type => dbms_tf.type_varchar2), 
        2 => dbms_tf.column_metadata_t(name => 'COLUMN_VALUE', type => dbms_tf.type_number)
      ), 
      row_replication => true
    ); 
  END;
  
  PROCEDURE fetch_rows  AS 
    env    dbms_tf.env_t;
    rowset dbms_tf.row_set_t; 
    colcnt PLS_INTEGER; 
    rowcnt PLS_INTEGER; 
    repfac dbms_tf.tab_naturaln_t; 
    namcol dbms_tf.tab_varchar2_t; 
    valcol dbms_tf.tab_number_t;  
  BEGIN
    env := dbms_tf.get_env();
    dbms_tf.get_row_set(rowset, rowcnt, colcnt); 
    
    FOR i IN 1 .. rowcnt LOOP repfac(i) := 0; END LOOP; 
    
    FOR r IN 1 .. rowcnt LOOP 
      FOR c IN 1 .. colcnt LOOP 
        IF rowset(c).tab_number(r) IS NOT NULL THEN 
          repfac(r) := repfac(r) + 1; 
          namcol(nvl(namcol.last+1,1)) :=  initcap(regexp_replace(env.get_columns(c).name, '^"|"$')); 
          valcol(nvl(valcol.last+1,1)) := rowset(c).tab_number(r); 
        END IF; 
      END LOOP; 
    END LOOP; 
    
    dbms_tf.row_replication(replication_factor => repfac); 
    dbms_tf.put_col(1, namcol); 
    dbms_tf.put_col(2, valcol); 
  END; 
  
END pkg_ptf_stack;
/

CREATE OR REPLACE FUNCTION ptf_stack(tab TABLE, col COLUMNS) RETURN TABLE PIPELINED ROW POLYMORPHIC USING pkg_ptf_stack;
/

SELECT department_id, first_name, last_name, column_name, column_value
FROM ptf_stack(hr.employees, COLUMNS(manager_id, salary, commission_pct))
WHERE department_id IN (90, 100)
ORDER BY department_id, first_name, last_name;
