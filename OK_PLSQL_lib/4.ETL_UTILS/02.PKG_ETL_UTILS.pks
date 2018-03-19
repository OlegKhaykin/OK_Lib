prompt Creating package PKG_ETL_UTILS
 
CREATE OR REPLACE PACKAGE pkg_etl_utils AS
/*
  Package ETL_UTILS contains procedures for performing data transformation operations:
  add data or delete data to/from target tables based on the content of the source views.

  History of changes (newest to oldest):
  ------------------------------------------------------------------------------
  02-FEB-2018, OK: added parameter P_DELETE_CND to ADD_DATA procedure;
  01-FEB-2018, OK: added parameter P_CHANGES_ONLY to ADD_DATA procedure;
  10-Nov-2015, OK: new version
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
  );
 
  -- Function GET_COL_LIST returns a comma-separated list of all the table column names.
  FUNCTION get_col_list(p_table IN VARCHAR2) RETURN VARCHAR2;
 
  -- Function GET_KEY_COL_LIST returns a comma-separated list of the table key column names.
  -- By default, describes the table PK.
  -- Optionally, can describe the given UK,
  FUNCTION get_key_col_list
  (
    p_table IN VARCHAR2,
    p_key   IN VARCHAR2 DEFAULT NULL -- optional name of the UK to be described
  ) RETURN VARCHAR2;

 
  -- Function GET_COLUMN_INFO returns a table-like structure with descriptions of all the table columns.
  -- See definition of the type RESULTSSTAGING.OBJ_COLUMN_INFO.
  FUNCTION get_column_info(p_table IN VARCHAR2) RETURN tab_column_info PIPELINED;


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
    p_delete_cnd    IN VARCHAR2 DEFAULT NULL, -- if specified, the MERGE operation will delete the target table rows if the matching source rows satisfy this condition 
    p_add_cnt       IN OUT PLS_INTEGER, -- number of added/changed rows
    p_err_cnt       IN OUT PLS_INTEGER  -- number of errors
  );
 

  -- "Silent" version of the previous procedure - i.e. with no OUT parameters
  PROCEDURE add_data
  (
    p_operation     IN VARCHAR2, -- 'MERGE', 'APPEND', 'INSERT' 'REPLACE' or 'EQUALIZE'
    p_tgt           IN VARCHAR2, -- target table to add rows to
    p_src           IN VARCHAR2, -- source table or view
    p_whr           IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to p_src
    p_errtab        IN VARCHAR2 DEFAULT NULL, -- optional error log table
    p_hint          IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at     IN NUMBER   DEFAULT 0, -- 0 - do not commit, otherwise commit
    p_uk_col_list   IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use in MERGE operation instead of PK columns
    p_changes_only  IN VARCHAR2 DEFAULT 'N', -- if 'Y', the MERGE operation should check that at least one non-key value will be changed
    p_delete_cnd    IN VARCHAR2 DEFAULT NULL -- if specified, the MERGE operation will delete the target table rows if the matching source rows satisfy these condition 
  );


  -- Procedure DELETE_DATA deletes from the Target table (P_TGT)
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
    p_not_in      IN VARCHAR2 DEFAULT 'N',
    p_del_cnt     IN OUT PLS_INTEGER -- number of deleted rows
  );

  -- "Silent" version - i.e. with no OUT parameter
  PROCEDURE delete_data
  (
    p_tgt         IN VARCHAR2, -- target table to delete rows from
    p_src         IN VARCHAR2, -- list of rows to be deleted
    p_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to p_src
    p_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    p_commit_at   IN PLS_INTEGER DEFAULT 0,
    p_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    p_not_in      IN VARCHAR2 DEFAULT 'N'
  );
END;
/

CREATE OR REPLACE SYNONYM etl FOR pkg_etl_utils;
GRANT EXECUTE ON etl TO PUBLIC;
