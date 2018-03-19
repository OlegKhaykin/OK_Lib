prompt Creating package body PKG_ETL_UTILS

CREATE OR REPLACE PACKAGE BODY pkg_etl_utils AS
/*
  Package for performing ETL tasks
  History of changes (newest to oldest):
  ------------------------------------------------------------------------------
  14-FEB-2018, OK: simplified MERGE statement in ADD_DATA procedure;
  02-FEB-2018, OK: added parameter P_DELETE_CND to ADD_DATA procedure;
  01-FEB-2018, OK: added parameter P_CHANGES_ONLY to ADD_DATA procedure;
*/
  -- Procedure RESOLVE_NAME resolves the given table/view/synonym name
  -- into complete description of the underlying table/view:
  -- schema, table/view name, DB link
  PROCEDURE resolve_name
  (
    p_name    IN  VARCHAR2,
    p_schema  OUT VARCHAR2,
    p_table   OUT VARCHAR2,
    p_db_link OUT VARCHAR2
  ) IS
    l_name  VARCHAR2(92);
    l       PLS_INTEGER;
    n       PLS_INTEGER;
    m       PLS_INTEGER;
  BEGIN
    l_name := UPPER(p_name);
    n := INSTR(l_name, '.');
    m := INSTR(l_name, '@');
    l := LENGTH(l_name);
 
    IF n>0 OR m>0 THEN
      IF n>0 THEN
        p_schema := SUBSTR(l_name, 1, n-1);
        p_table := SUBSTR(l_name, n+1, CASE WHEN m>0 THEN m-n-1 ELSE l END);
        p_db_link := CASE WHEN m>0 THEN SUBSTR(l_name, m) END;
      ELSE
        p_table := SUBSTR(l_name, 1, m-1);
        p_db_link := SUBSTR(l_name, m);
        SELECT username INTO p_schema FROM user_db_links WHERE db_link = UPPER(SUBSTR(p_db_link, 2));
      END IF;
 
    ELSE
      SELECT table_owner, table_name, db_link
      INTO p_schema, p_table, p_db_link
      FROM
      (
        SELECT
          SYS_CONTEXT('USERENV','CURRENT_SCHEMA') table_owner,
          object_name table_name,
          NULL db_link
        FROM user_objects
        WHERE object_type IN ('TABLE','VIEW') AND object_name = UPPER(p_name)
        UNION ALL
        SELECT
          table_owner,
          table_name,
          NVL2(db_link, '@'||db_link, NULL) db_link
        FROM user_synonyms
        WHERE synonym_name = UPPER(p_name)
      );
    END IF;
  EXCEPTION
   WHEN NO_DATA_FOUND THEN
    Raise_Application_Error(-20000, 'Unknown table/view: '||p_name);
  END;
 
  
  -- Function GET_COL_LIST returns a comma-separated list of all the table column names.
  FUNCTION get_col_list(p_table IN VARCHAR2) RETURN VARCHAR2 IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
    ret       VARCHAR2(1000);
  BEGIN
    resolve_name(p_table, l_schema, l_tname, l_dblink);
   
    SELECT concat_v2_set
    (
      CURSOR
      (
        SELECT column_name
        FROM all_tab_cols
        WHERE owner = l_schema
        AND table_name = l_tname
        ORDER BY column_id
      )
    ) INTO ret FROM dual;
         
    RETURN ret;
  END;
 
  -- Function GET_KEY_COL_LIST returns a comma-separated list of the table key column names.
  -- By default, describes the table PK.
  -- Optionally, can describe the given UK,
  FUNCTION get_key_col_list(p_table IN VARCHAR2, p_key IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
    ret       VARCHAR2(1000);
  BEGIN
    resolve_name(p_table, l_schema, l_tname, l_dblink);
   
    SELECT concat_v2_set
    (
      CURSOR
      (
        SELECT cc.column_name
        FROM all_constraints c
        JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
        WHERE c.owner = l_schema AND c.table_name = l_tname
        AND
        (
          c.constraint_type = 'P' AND p_key IS NULL
          OR
          c.constraint_name = p_key
        )
        ORDER BY cc.position
      )
    ) INTO ret FROM dual;
         
    RETURN ret;
  END;
 
  -- Function GET_COLUMN_INFO returns a table-like structure with descriptions of all the table columns.
  -- See definition of the type RESULTSSTAGING.OBJ_COLUMN_INFO.
  FUNCTION get_column_info(p_table IN VARCHAR2) RETURN tab_column_info PIPELINED IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
  BEGIN
    resolve_name(p_table, l_schema, l_tname, l_dblink);
   
    FOR r IN
    (
      SELECT
         c.owner, c.table_name, c.column_id, c.column_name, 
         c.data_type, c.data_length, c.data_precision, c.data_scale, 
         c.nullable, NVL2(cc.column_name, 'Y', 'N') pk
      FROM all_tab_columns c
      LEFT JOIN all_constraints pk
        ON pk.owner = c.owner AND pk.table_name = c.table_name AND pk.constraint_type = 'P'
      LEFT JOIN all_cons_columns cc 
        ON cc.owner = pk.owner AND cc.constraint_name = pk.constraint_name AND cc.column_name = c.column_name
      WHERE c.owner = l_schema AND c.table_name = l_tname
      ORDER BY column_id 
    )
    LOOP
      PIPE ROW(obj_column_info
      (
        r.owner, r.table_name, r.column_id, r.column_name,
        r.data_type, r.data_length, r.data_precision, r.data_scale,
        r.nullable, r.pk
      ));
    END LOOP;
  END;
 
  -- Procedure ADD_DATA selects data from the specified source table or view (P_SRC)
  -- using optional WHERE condition (P_WHR).
  -- Depending on P_OPERATION, it either merges or inserts the source data into the Target table (P_TGT).
  -- The output parameter P_ADD_CNT gets the number of rows added to the target table.
  -- P_ERR_CNT gets the number of source rows that were rejected and placed in the error table (P_ERRTAB).
  -- Note: if P_ERRTAB is not specified, then this procedure errors-out
  -- if at least one source row cannot be placed into the target table.
  PROCEDURE add_data
  (
    p_operation     IN VARCHAR2, -- 'INSERT', 'UPDATE', 'MERGE', 'APPEND', 'REPLACE' or 'EQUALIZE'
    p_tgt           IN VARCHAR2, -- target table to add rows to
    p_src           IN VARCHAR2, -- source table or view
    p_whr           IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to p_src
    p_errtab        IN VARCHAR2 DEFAULT NULL, -- optional error log table,
    p_hint          IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at     IN NUMBER   DEFAULT 0, -- 0 - do not commit, otherwise commit
    p_uk_col_list   IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use in MERGE operation instead of PK columns
    p_changes_only  IN VARCHAR2 DEFAULT 'N', -- if 'Y', the MERGE operation should check that at least one non-key value will be changed
    p_delete_cnd    IN VARCHAR2 DEFAULT NULL, -- if specified, the MERGE operation will delete the target table rows if the matching source rows satisfy these condition 
    p_add_cnt       IN OUT PLS_INTEGER, -- number of added/changed rows
    p_err_cnt       IN OUT PLS_INTEGER  -- number of errors
  ) IS
    l_operation     VARCHAR2(8);
    l_view_name     VARCHAR2(30);
    l_src_schema    VARCHAR2(30);
    l_src_tname     VARCHAR2(61);
    l_src_db        VARCHAR2(30);
    l_tgt_schema    VARCHAR2(30);
    l_tgt_tname     VARCHAR2(30);
    l_tgt_db        VARCHAR2(30);
    l_err_schema    VARCHAR2(30);
    l_err_tname     VARCHAR2(30);
    l_err_db        VARCHAR2(30);
    l_pk_cols       VARCHAR2(200);
    l_on_list       VARCHAR2(500);
    l_ins_cols      VARCHAR2(20000);
    l_upd_cols      VARCHAR2(20000);
    l_hint1         VARCHAR2(500);
    l_hint2         VARCHAR2(500);
    l_cmd           VARCHAR2(32000);
    l_cnt           PLS_INTEGER;
    l_tag           VARCHAR2(30);
    l_ts            TIMESTAMP;
    l_act         VARCHAR2(256);
   
    -- Procedure COLLECT_METADATA gathers information about the columns
    -- of the target table and the source table/view.
    -- Gatherd information is stored in TMP_COLUMN_INFO and
    -- then used in dynamic DML statement generation.
    PROCEDURE collect_metadata IS
      PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
      l_cmd := 'INSERT INTO tmp_all_columns(side, owner, table_name, column_id, column_name, data_type, uk, nullable)
      SELECT ''SRC'', cl.owner, cl.table_name, cl.column_id, cl.column_name, cl.data_type, ''N'', ''Y''
      FROM all_tab_columns'||l_src_db||' cl
      WHERE cl.owner = '''||l_src_schema||''' AND cl.table_name = '''||l_src_tname||'''
      UNION
      SELECT ''TGT'', cl.owner, cl.table_name, cl.column_id, cl.column_name, cl.data_type, NVL2(cc.column_name, ''Y'', ''N'') uk, cl.nullable
      FROM all_tab_columns cl' ||
      CASE WHEN p_uk_col_list IS NOT NULL THEN '
      LEFT JOIN
      (
        SELECT VALUE(t) column_name
        FROM TABLE(split_string(UPPER('''||p_uk_col_list||'''))) t
      ) cc ON cc.column_name = cl.column_name'
      ELSE '
      LEFT JOIN all_constraints c
        ON c.owner = cl.owner AND c.table_name = cl.table_name
       AND c.constraint_type = ''P''
      LEFT JOIN all_cons_columns cc
        ON cc.owner = c.owner
       AND cc.constraint_name = c.constraint_name
       AND cc.column_name = cl.column_name '
      END || '
      WHERE cl.owner = '''||l_tgt_schema||''' AND cl.table_name = '''||l_tgt_tname||'''';
     
      EXECUTE IMMEDIATE l_cmd;
      l_cmd := NULL;
      
      SELECT
        concat_v2_set(CURSOR(
          SELECT column_name
          FROM tmp_all_columns
          WHERE side = 'TGT' AND uk = 'Y'
          ORDER BY column_id)
        ),
        concat_v2_set(CURSOR(
          SELECT
            CASE nullable
              WHEN 'N' THEN 't.'||column_name||'=q.'||column_name
              ELSE CASE
                WHEN data_type LIKE '%CHAR%' THEN
                  'NVL(t.'||column_name||', ''$$N/A$$'') = NVL(q.'||column_name||', ''$$N/A$$'')'
                WHEN data_type = 'DATE' THEN
                  'NVL(t.'||column_name||', DATE ''0001-01-01'') = NVL(q.'||column_name||', DATE ''0001-01-01'')'
                WHEN data_type LIKE 'TIME%' THEN
                  'NVL(t.'||column_name||', TIMESTAMP ''0001-01-01 00:00:00'') = NVL(q.'||column_name||', TIMESTAMP ''0001-01-01 00:00:00'')'
                ELSE 'NVL(t.'||column_name||', -101010101) = NVL(q.'||column_name||', -101010101)'
              END
            END
          FROM tmp_all_columns
          WHERE side = 'TGT' AND uk = 'Y'
          ORDER BY column_id), ' AND '
        ),
        concat_v2_set(CURSOR(
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'TGT'
          INTERSECT
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'SRC'
          MINUS
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'TGT' AND uk = 'Y')
        ),
        concat_v2_set(CURSOR(
          SELECT 'q.'||tc.column_name
          FROM tmp_all_columns tc
          JOIN tmp_all_columns sc ON sc.column_name = tc.column_name
          WHERE tc.side = 'TGT' AND sc.side = 'SRC'
          ORDER BY tc.column_id)
        )
      INTO l_pk_cols, l_on_list, l_upd_cols, l_ins_cols
      FROM dual;
 
      COMMIT; -- to delete rows from TMP_ALL_COLUMNS
    END;
  BEGIN
    xl.begin_action('Adding data to '||p_tgt, 'Operation: '||p_operation);
    
    l_cnt := INSTR(p_operation, ' ');
    IF l_cnt = 0 THEN l_cnt := LENGTH(p_operation); END IF;
   
    l_operation := RTRIM(UPPER(SUBSTR(p_operation, 1, l_cnt)));
   
    l_hint1 := SUBSTR(p_operation, l_cnt+1);
 
    IF l_operation NOT IN ('INSERT', 'UPDATE', 'MERGE', 'REPLACE', 'EQUALIZE') THEN
      Raise_Application_Error(-20000, 'Unsupported operation: '||l_operation);
    END IF;
    
    IF p_hint IS NOT NULL THEN
      l_hint2 := '/*+ '||p_hint||' */';
    END IF;
    
    IF UPPER(p_src) LIKE '%SELECT%' THEN
      l_view_name := 'ETL_'||xl.get_current_proc_id;
      l_src_tname := l_view_name;
      l_cmd := 'CREATE VIEW '||l_view_name||' AS '||p_src;
      xl.begin_action('Creating view '||l_view_name, l_cmd);
      EXECUTE IMMEDIATE l_cmd;
      xl.end_action;
    ELSE
      l_src_tname := p_src;
    END IF;
    
    resolve_name(l_src_tname, l_src_schema, l_src_tname, l_src_db);
    resolve_name(p_tgt, l_tgt_schema, l_tgt_tname, l_tgt_db);
    IF p_errtab IS NOT NULL THEN
      resolve_name(p_errtab, l_err_schema, l_err_tname, l_err_db);
    END IF;
   
    collect_metadata;
    
    IF l_ins_cols IS NULL THEN
      Raise_Application_Error
      (
        -20000,
        'No common columns found for the source and target tables. '||
        'Check that you have access to both of them and they have matching columns.'
      );
    END IF;
   
    IF l_operation IN ('UPDATE', 'MERGE', 'EQUALIZE') AND l_pk_cols IS NULL THEN -- OK-130808
      Raise_Application_Error(-20000, 'No Key specified for '||l_tgt_schema||'.'||l_tgt_tname);
    END IF;
   
    l_tag := NVL(TO_CHAR(xl.get_current_proc_id), SYS_CONTEXT('USERENV','SESSIONID'));
    l_ts := SYSTIMESTAMP;
    
    IF l_operation = 'REPLACE' THEN
      xl.begin_action('Truncating table '||p_tgt);
      EXECUTE IMMEDIATE 'TRUNCATE TABLE '||p_tgt;
      l_operation := 'INSERT';
      xl.end_action;
 
    ELSIF l_operation = 'EQUALIZE' THEN
      xl.begin_action('Deleting extra data from '||p_tgt);
        l_cmd := 'DELETE '||l_hint1||' FROM '||p_tgt||'
        WHERE ('||l_pk_cols||') NOT IN
        (
          SELECT '||l_pk_cols||'
          FROM '||l_src_tname||' '||p_whr||'
        )';
        
        xl.begin_action('Executing command', l_cmd);
        EXECUTE IMMEDIATE l_cmd;
        l_cnt := SQL%ROWCOUNT;
        xl.end_action(l_cnt||' rows deleted');
        l_operation := 'MERGE';
      xl.end_action;
    END IF;
   
    IF l_operation = 'INSERT' AND p_commit_at > 0 THEN -- incremental insert with commit afrer each portion
      l_cmd := '
      DECLARE
        CURSOR cur IS
        SELECT '||l_hint2||' '||REPLACE(LOWER(l_ins_cols), 'q.')||' FROM '||l_src_tname||' q '||p_whr||';
         
        TYPE buffer_type IS TABLE OF cur%ROWTYPE;
         
        bfr  buffer_type;
        cnt  PLS_INTEGER;
      BEGIN
        :add_cnt := 0;
       
        OPEN cur;
        LOOP
          FETCH cur BULK COLLECT INTO bfr LIMIT :commit_at;
          cnt := bfr.COUNT;
 
          FORALL i IN 1..cnt
          INSERT INTO '||p_tgt||'('||REPLACE(LOWER(l_ins_cols), 'q.')||')
          VALUES('||REPLACE(LOWER(l_ins_cols), 'q.', 'bfr(i).')||')'||
          CASE WHEN l_err_tname IS NOT NULL THEN '
          LOG ERRORS INTO '||l_err_schema||'.'||l_err_tname||' (:tag) REJECT LIMIT UNLIMITED' END||';
 
          :add_cnt := :add_cnt + SQL%ROWCOUNT;
          COMMIT;
 
          IF cnt < :commit_at THEN
            EXIT;
          END IF;
 
          xl.end_action(:add_cnt||'' rows inserted so far'');
          xl.begin_action(:act, ''Continue ...'');
        END LOOP;
        CLOSE cur;
      END;';
     
      l_act := 'Inserting rows by portions';
      xl.begin_action(l_act, l_cmd);
        IF l_err_tname IS NOT NULL THEN
          EXECUTE IMMEDIATE l_cmd USING IN OUT p_add_cnt, p_commit_at, l_tag, l_act;
        ELSE
          EXECUTE IMMEDIATE l_cmd USING IN OUT p_add_cnt, p_commit_at, l_act;
        END IF;
        l_cmd := NULL;
      xl.end_action('Totally inserted: '||p_add_cnt||' rows');
 
    ELSE -- "one-shot" load with or without commit
      l_cmd :=
      CASE WHEN l_operation IN ('UPDATE', 'MERGE') THEN '
      MERGE '||l_hint1||' INTO '||p_tgt||' t USING ' || 
        CASE WHEN p_whr IS NOT NULL OR l_hint2 IS NOT NULL THEN '
      (
        SELECT '||l_hint2||' *
        FROM '||NVL(l_view_name, p_src)||' '||p_whr||'
      )'
        ELSE NVL(l_view_name, p_src)
        END || ' q
      ON ('||l_on_list||')'||
        CASE WHEN l_upd_cols IS NOT NULL THEN '
      WHEN MATCHED THEN UPDATE SET '||l_upd_cols||
          CASE WHEN p_changes_only IN ('Y', 'y') THEN '
      WHERE '||REPLACE(REGEXP_REPLACE(l_upd_cols, '(t\.[^=]+=q\.[^,]+)', 'LNNVL(\1)', 1, 0), ',', ' OR ')
          END ||
          CASE WHEN p_delete_cnd IS NOT NULL THEN '
      DELETE WHERE '||p_delete_cnd
          END 
        END ||
        CASE WHEN l_operation = 'MERGE' THEN '
      WHEN NOT MATCHED THEN INSERT ('||REPLACE(l_ins_cols, 'q.')||') VALUES ('||l_ins_cols||')'
        END
      ELSE '
      INSERT '||l_hint1||'
      INTO '||p_tgt||'('||REPLACE(l_ins_cols, 'q.')||')
      SELECT '||l_hint2||' '||l_ins_cols||' FROM '||l_src_tname||' q '||p_whr
      END ||
      CASE WHEN l_err_tname IS NOT NULL THEN '
      LOG ERRORS INTO '||l_err_schema||'.'||l_err_tname||' (:tag) REJECT LIMIT UNLIMITED'
      END;
      
      xl.begin_action('Executing command', l_cmd);
        IF l_err_tname IS NOT NULL THEN
          EXECUTE IMMEDIATE l_cmd USING l_tag;
          p_add_cnt := SQL%ROWCOUNT;
        ELSE
          EXECUTE IMMEDIATE l_cmd;
          p_add_cnt := SQL%ROWCOUNT;
        END IF;
        l_cmd := NULL;
      xl.end_action;
      
      IF p_commit_at <> 0 THEN
        COMMIT;
      END IF;
    END IF;
    
    IF l_err_tname IS NOT NULL THEN
      EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM '||l_err_schema||'.'||l_err_tname||' WHERE ora_err_tag$ = :tag AND entry_ts >= :ts'
      INTO p_err_cnt USING l_tag, l_ts;
    ELSE
      p_err_cnt := 0;
    END IF;
    
    IF l_view_name IS NOT NULL THEN
      l_cmd := 'DROP VIEW '||l_view_name;
      xl.begin_action('Dropping view '||l_view_name, l_cmd);
      EXECUTE IMMEDIATE l_cmd;
      xl.end_action;
    END IF; 
    
    xl.end_action(p_add_cnt||' rows added; '||p_err_cnt||' errors found');
  END;
  
  -- "Silent" version - with no OUT parameters
  PROCEDURE add_data
  (
    p_operation   IN VARCHAR2, -- 'MERGE', 'APPEND', 'INSERT' 'REPLACE' or 'EQUALIZE'
    p_tgt         IN VARCHAR2, -- target table to add rows to
    p_src         IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    p_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to p_src
    p_errtab      IN VARCHAR2 DEFAULT NULL, -- optional error log table,
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at   IN NUMBER   DEFAULT 0, -- 0 - do not commit, otherwise commit
    p_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use in MERGE operation instead of PK columns
    p_changes_only  IN VARCHAR2 DEFAULT 'N', -- if 'Y', the MERGE operation should check that at least one non-key value will be changed
    p_delete_cnd    IN VARCHAR2 DEFAULT NULL -- if specified, the MERGE operation will delete the target table rows if the matching source rows satisfy this condition 
  ) IS
    l_add_cnt PLS_INTEGER;
    l_err_cnt PLS_INTEGER;
  BEGIN
    add_data
    (
      p_operation, p_tgt, p_src, p_whr, p_errtab, p_hint, p_commit_at,
      p_uk_col_list, p_changes_only, p_delete_cnd, l_add_cnt, l_err_cnt
    );
  END;
 
  -- Procedure DELETE_DATA deletes from the target table (P_TGT)
  -- the data that exists (P_NOT_IN='N') or not exists (P_NOT_IN='Y')
  -- in the source table/view (P_SRC)
  -- matching rows by either all the columns of the target table Primary Key (default)
  -- or by the given list of unique columns (P_UK_COL_LIST).
  PROCEDURE delete_data
  (
    p_tgt         IN VARCHAR2, -- target table to delete rows from
    p_src         IN VARCHAR2, -- list of rows to be deleted
    p_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to p_src
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at   IN PLS_INTEGER DEFAULT 0,
    p_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    p_not_in      IN VARCHAR2 DEFAULT 'N', -- if "Y' then the condition is "NOT IN"
    p_del_cnt     IN OUT PLS_INTEGER -- number of deleted rows
  ) IS
    l_pk_cols     VARCHAR2(2000);
    l_hint        VARCHAR2(500);
    l_cmd         VARCHAR2(4000);
   
  BEGIN
    xl.begin_action('Deleting data from '||p_tgt);
   
    IF p_uk_col_list IS NOT NULL THEN
      l_pk_cols := p_uk_col_list;
     
    ELSE
      l_pk_cols := get_key_col_list(p_tgt);
 
      IF l_pk_cols IS NULL THEN
        Raise_Application_Error(-20000,'No Pimary Key specified for '||p_tgt);
      END IF;
    END IF;
   
    IF p_hint IS NOT NULL THEN
      l_hint := '/*+ '||p_hint||' */';
    END IF;
 
    l_cmd := '
    DELETE FROM '||p_tgt||' WHERE ('||REPLACE(l_pk_cols, 'q.')||') '|| CASE p_not_in WHEN 'Y' THEN 'NOT ' END||'IN
    (
      SELECT '||l_hint||' '||l_pk_cols||' FROM '||p_src||' q '||p_whr|| '
    )';
    xl.begin_action('Executing command', l_cmd);
    EXECUTE IMMEDIATE l_cmd;
    p_del_cnt := SQL%ROWCOUNT;
    xl.end_action;
   
    IF p_commit_at <> 0 THEN
      COMMIT;
    END IF;
    
    xl.end_action(p_del_cnt||' rows deleted');
  END;
  
  -- "Silent" version - i.e. with no OUT parameter
  PROCEDURE delete_data
  (
    p_tgt         IN VARCHAR2, -- target table to delete rows from
    p_src         IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    p_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to I_SRC
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at   IN PLS_INTEGER DEFAULT 0,
    p_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    p_not_in      IN VARCHAR2 DEFAULT 'N'
  ) IS
    l_del_cnt PLS_INTEGER;
  BEGIN
    delete_data(p_tgt, p_src, p_whr, p_hint, p_commit_at, p_uk_col_list, p_not_in, l_del_cnt);
  END;
END;
/
