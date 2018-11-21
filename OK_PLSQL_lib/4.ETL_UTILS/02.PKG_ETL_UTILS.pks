prompt Creating package PKG_ETL_UTILS
 
CREATE OR REPLACE PACKAGE pkg_etl_utils_new AS
/*
  =============================================================================
  Package ETL_UTILS contains procedures for performing data transformation operations:
  add data or delete data to/from target tables based on the content of the source views.

  It was developed by Oleg Khaykin. 1-201-625-3161. OlegKhaykin@gmail.com. 
  Your are allowed to use and change it as you wish, as long as you
  retain here the reference to the original developer - i.e. Oleg Khaykin.
  
  In other words, you are not allowed to remove this comment!
 
  History of changes (newest to oldest):
  ------------------------------------------------------------------------------
  20-Nov-2018, OK: new version
  =============================================================================

  Procedure ADD_DATA selects data from the specified source table/view/query (P_SOURCE)
  optionally applying the filter condition P_WHERE.
  Depending on P_OPERATION, it either inserts all or merges the new/changed
  source rows into the target table (P_TARGET).
  The source and the target columns are matched by their names, not by their positions.
  If P_OPERATION = 'REPLACE', then the procedure first truncates the target table
  and then inserts all the source rows.
  
  Parameters:
  ----------------------------------------------------------------------
  - P_MATCH_COLS - a comma-separated list of the columns on which to match
    the source and the target rows during MERGE/UPDATE operations.
    If not specified, then matching is done on all the columns
    of the target table's Primary Key.
    
    In cases when the source query/view pre-joins the target table,
    it is recommended to include into the source dataset the column ROW_ID
    holding the target table's ROWIDs.
    Then, with the parameter P_MATCH_COLS => 'ROWID', the target (t) 
    and the source (s) will be matched on t.ROWID = s.row_id.
    In this case, you should also include in the source view/query these columns:
    - ETL$SRC_INDICATOR - if you are using NOTFOUND deletion condition; 
      see below the description of the P_DELETE parameter;
    - "Delete Flag" column - if you are using logical deletion;
    - "Version Number" column - if you have such column in the versioned target table;
      see below the description of the P_VERSIONS parameter;
    
  - P_CHECK_CHANGED:
    - 'ALL' - this is the default value - check all the non-match-key columns
      for changes; perform update only if some of these columns have changed -
      i.e. their values on the source row are different from the values on the matching target row.
    - 'NONE' - do not check for changes, perform update regardless.
    - '<comma-separated list of columns>' - check for changes only the listed columns,
      perform updates only if some of them have changed.
    - 'EXCEPT <list of columns>' - check for changes all the non-match-key columns
      except the listed ones.
      
  - P_GENERATE should be in the form:
    <column 1>, <column 2> ... = <expression 1>, <expression 2> ...
    where <column 1> ... <column *> should be replaced with actual target table
    column names and actual expressions should be used.
    
    For example:
    q'[vid, cob_date = my_sequence.NEXTVAL, SYS_CONTEXT('IDL_CONTEXT','COB_DATE')]'
    Lower/Upper case is important only in string literals. Spaces around "=" and "," are optional.
    
    This data generation occurs ONLY ON THE NEWLY INSERTED ROWS, not on the updated ones!
    It is mainly used for generating surrogate keys from sequences.
    The other column - COB_DATE - could be included into the source view/query instead,
    in which case its value would be used ON BOTH THE NEWLY INSERTED ROWS AND THE UPDATED ONES.
    
  - P_DELETE - defines deletion logic for UPDATE and MERGE operations.
    It has the form: IF <condition> THEN <action>.
    - The <condition> can be either:
      a) NOTFOUND - meaning that there is no matching row in the source;
      b) Any expression that can be evaluated to TRUE or FALSE.
         It is allowed to refer here to the source columns: s.<column name>.
    - If "THEN <action>" is omitted then the procedure will actually delete 
      the qualifying rows from the target table.
      Otherwise, <action> should have the form:
      <deleted flag column>=<deleted value>:<active value>,
      in which case the qualifying target rows will be logically deleted or un-deleted
      by setting the <deleted flag column> to <deleted value> if <condition> is TRUE
      or to <active value> if <condition> is FALSE.
    For example:
    1) 'IF NOTFOUND' - delete from the target table all the rows
        that do not have matching source rows.
    2) q['IF s.delete_flag='Y']' - delete from the target table the rows
       for which the matching source rows have DELETE_FLAG='Y'.
    3) 'IF NOTFOUND THEN active_indicator=0:1' - set ACTIVE_INDICATOR=0
       on all the target rows for which there are no matching source rows,
       set ACTIVE_INDICATOR=1 on all the matching target rows and on the newly
       inserted ones.
    
    It is not allowed to have delete flags/indictors on both source and target sides:
    q'[IF s.delete_flag='Y' THEN active_indicator=0:1]'
    In this case, instead of using P_DELETE parameter, you should include
    the ACTIVE_INDICATOR column into the source:
    CASE WHEN s.delete_flag = 'Y' THEN 0 ELSE 1 END AS active_indicator 
    
    If you are using NOTFOUND condition together with ROWID matching
    (see P_MATCH_COLS above), then your source view/query should have the column
    ETL$SRC_INDICATOR having value 1 when the source row exists and being NULL otherwise.
    To achieve that, your source query/view should use either FULL JOIN
    or OUTER JOIN with the target table being the driving one.
    
  - P_VERSIONS defines versioning logic. The complete form is:
    <version column>; <from column>=<from expession>; <until column>=<until expression>  
    where <version column>, <from column> and <until column> should be replaced
    with actual names of the corresonding columns. For example:
    q'[VERSION_NUM; START_TS=SYSDATE; END_TS=SYSDATE - INTERVAL '1' SECOND]'
    
    If P_VERSIONS is specified, then instead of updating the matching
    "current" rows in the target table, the procedure will "end-date" them
    by setting <until column>=<until expression> and will insert new rows
    with new attribute values, with <version column> (if any) increased by 1,
    with <from column>=<from expression> and with <until column> being set to
    either '31-DEC-9999' or NULL - depending on whether or not that column is mandatory.
    Here, "current" rows are those with <until column> being either '31-DEC-9999' or NULL.
    
    The target table may not have a version number column. In that case,
    <version column> should be omitted in P_VERSIONS together with the following ";".
    The From/Until expressions can be omitted as well together with the preceding "=".
    For example: 'START_DT;END_DT'
    In this case, the version numbers will not be assigned and the default
    From/Until expressions will be used: TRUNC(SYSDATE) and TRUNC(SYSDATE)-1 respectively.
    
    If you are using <version column> together with ROWID matching,
    then you should include this column into your source query/view.
    
    Note: with versioning, you cannot physically delete data!
    Therefore, if you need, you must use logical deletion: 
    i.e in the P_DELETE condition you must have "THEN <action>" or, 
    you should have a deleted indicator column in the source and not use P_DELETE at all.
  
  - P_COMMIT_AT: 0 - do not commit, negative number - commit once at the end,
                 positive N - commit after processing every N source rows.
    
  - P_ADD_CNT - this output parameter gets the number of the added+changed+deleted rows.
  - P_ERR_CNT - gets the number of the source rows that have been rejected and placed into the error table (P_ERRTAB).
  - If P_ERRTAB is not specified then the whole transaction is cancelled and the procedure errors-out in case of even one error.
*/
  PROCEDURE add_data
  (
    p_operation       IN VARCHAR2, -- 'INSERT', 'UPDATE', 'MERGE' or 'REPLACE'
    p_target          IN VARCHAR2, -- target table to add rows to
    p_source          IN VARCHAR2, -- source table, view or query
    p_where           IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to the source 
    p_hint            IN VARCHAR2 DEFAULT NULL, -- optional hint to apply to the source 
    p_match_cols      IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above 
    p_check_changed   IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_generate        IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_delete          IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_versions        IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above 
    p_commit_at       IN NUMBER   DEFAULT 0,    -- see in the procedure description above
    p_errtab          IN VARCHAR2 DEFAULT NULL, -- optional error log table,
    p_add_cnt         IN OUT PLS_INTEGER,       -- number of added/changed/deleted rows
    p_err_cnt         IN OUT PLS_INTEGER        -- number of errors
  );
 
  -- "Silent" version of the previous procedure - i.e. with no OUT parameters
  PROCEDURE add_data
  (
    p_operation       IN VARCHAR2, -- 'INSERT', 'UPDATE', 'MERGE' or 'REPLACE'
    p_target          IN VARCHAR2, -- target table to add rows to
    p_source          IN VARCHAR2, -- source table, view or query
    p_where           IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to the source 
    p_hint            IN VARCHAR2 DEFAULT NULL, -- optional hint to apply to the source 
    p_match_cols      IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_check_changed   IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_generate        IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_delete          IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above
    p_versions        IN VARCHAR2 DEFAULT NULL, -- see in the procedure description above 
    p_commit_at       IN NUMBER   DEFAULT 0,    -- see in the procedure description above
    p_errtab          IN VARCHAR2 DEFAULT NULL  -- optional error log table
  );
  
  
  -- Procedure DELETE_DATA deletes from the target table (P_TARGET)
  -- the data that exists (P_NOT_IN='N') or does not exists (P_NOT_IN='Y')
  -- in the source table/view (P_SOURCE) after applying to the source an
  -- optional filtering condition (P_WHERE), matching source and target rows
  -- by either all the columns of the target table Primary Key (default)
  -- or by the given list of columns (P_MATCH_COLS).
  PROCEDURE delete_data
  (
    p_target      IN VARCHAR2, -- target table to delete rows from
    p_source      IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    p_where       IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to the source
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_not_in      IN VARCHAR2 DEFAULT 'N', -- if 'N' then the source lists the rows to be deleted; if 'Y' - the rows to be preserved
    p_match_cols  IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    p_commit_at   IN PLS_INTEGER DEFAULT 0,
    p_del_cnt     IN OUT PLS_INTEGER -- number of deleted rows
  );

  -- "Silent" version - i.e. with no OUT parameter
  PROCEDURE delete_data
  (
    p_target      IN VARCHAR2, -- target table to delete rows from
    p_source      IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    p_where       IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to the source
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_not_in      IN VARCHAR2 DEFAULT 'N', -- if 'N' then the source lists the rows to be deleted; if 'Y' - the rows to be preserved
    p_match_cols  IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    p_commit_at   IN PLS_INTEGER DEFAULT 0
  );

  -- Procedure PARSE_NAME splits the given name into 3 pieces:
  -- 1) schema, 2) table/view name and 3) DB_link
  PROCEDURE parse_name
  (
    p_name    IN  VARCHAR2,
    p_schema  OUT VARCHAR2,
    p_table   OUT VARCHAR2,
    p_db_link OUT VARCHAR2
  );
  
  -- Procedure FIND_TABLE resolves the given table/view/synonym name
  -- into a complete SCHEMA.NAME description of the underlying table/view
  PROCEDURE find_table
  (
    p_name    IN  VARCHAR2,
    p_schema  OUT VARCHAR2,
    p_table   OUT VARCHAR2
  );
  
  -- This version of FIND_TABLE finds the actual SCHEMA.NAME 
  -- for the given SCHEMA.NAME, which can be a synonym
  PROCEDURE find_table
  (
    p_schema  IN OUT VARCHAR2,
    p_table   IN OUT VARCHAR2
  );
  
  -- Function GET_KEY_COL_LIST returns a comma-separated list of the table key column names.
  -- By default, describes the table PK.
  -- Optionally, can describe the given UK,
  FUNCTION get_key_col_list
  (
    p_table IN VARCHAR2,
    p_key   IN VARCHAR2 DEFAULT NULL -- optional name of the UK to be described
  ) RETURN VARCHAR2;
  
  -- Procedure SET_PARAMETER sets parameter value in the local ETL_CONTEXT namespace
  PROCEDURE set_parameter(p_name IN VARCHAR2, p_value IN VARCHAR2);
END;
/

CREATE OR REPLACE SYNONYM etl FOR pkg_etl_utils;
GRANT EXECUTE ON etl TO PUBLIC;