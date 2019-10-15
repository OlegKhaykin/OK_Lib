CREATE OR REPLACE PACKAGE pkg_db_maintenance AS
/*
  Description: this package contains procedures for various DB maintenance tasks.
*/
  TYPE rec_partition_info IS RECORD
  (
    table_owner         VARCHAR2(30),
    table_name          VARCHAR2(128),
    tablespace_name     VARCHAR2(30),
    partition_name      VARCHAR2(128),
    partition_position  NUMBER(6),
    high_value          VARCHAR2(255),
    compress_for        VARCHAR2(30),
    num_blocks          NUMBER(10),
    num_rows            INTEGER,
    last_analyzed       DATE
  );
 
  TYPE tab_partition_info IS TABLE OF rec_partition_info;
 
  TYPE rec_subpartition_info IS RECORD
  (
    table_owner           VARCHAR2(30),
    table_name            VARCHAR2(128),
    tablespace_name       VARCHAR2(30),
    partition_name        VARCHAR2(128),
    subpartition_name     VARCHAR2(128),
    subpartition_position NUMBER(6),
    high_value            VARCHAR2(255),
    compress_for          VARCHAR2(30),
    num_blocks            NUMBER(10),
    num_rows              INTEGER,
    last_analyzed         DATE
  );
 
  TYPE tab_subpartition_info IS TABLE OF rec_subpartition_info;
  
  TYPE tab_v256 IS TABLE OF VARCHAR2(256);
  
  FUNCTION split_string(p_string IN VARCHAR2, p_delimeter IN VARCHAR2 default ',') RETURN tab_v256 PIPELINED;
 
  FUNCTION get_partition_info
  (
    i_table_owner         IN VARCHAR2,
    i_table_name          IN VARCHAR2,
    i_partition_name      IN VARCHAR2 DEFAULT NULL,
    i_partition_position  IN NUMBER DEFAULT NULL
  ) RETURN tab_partition_info PIPELINED;
  
  FUNCTION get_subpartition_info
  (
    i_table_owner         IN VARCHAR2,
    i_table_name          IN VARCHAR2,
    i_partition_name      IN VARCHAR2 DEFAULT NULL
  ) RETURN tab_subpartition_info PIPELINED;
  
  PROCEDURE compress_data -- to compress data
  (
    i_condition         IN VARCHAR2,-- for example: 'WHERE table_name=''PATHSENS'' AND part_dt BETWEEN ''01-JAN-13'' AND ''31-DEC-13'';
    i_compress_type     IN VARCHAR2, -- for example: 'NOCOMPRESS', 'BASIC', 'OLTP', 'QUERY HIGH', etc.
    i_max_days          IN PLS_INTEGER DEFAULT NULL,
    i_update_indexes    IN CHAR DEFAULT 'N',
    i_gather_stats      IN CHAR DEFAULT 'N',
    i_deallocate_unused IN CHAR DEFAULT 'Y',
    i_tablespace        IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE deallocate_unused_space -- to reclaim unused disk space
  (
    i_condition       IN VARCHAR2 -- for example: 'WHERE table_name=''PATHSENS''';
  );
 
  PROCEDURE compress_indexes -- to rebuild partitioned indexes as COMPRESSED
  (
    i_table_list      IN VARCHAR2 DEFAULT NULL, -- comma-separated list of tables
    i_tablespace_name IN VARCHAR2,
    i_force           IN CHAR DEFAULT 'N'
  );
 
  PROCEDURE disable_index_partitions
  (
    i_condition IN VARCHAR2,
    i_comment   IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE enable_index_partitions
  (
    i_condition IN VARCHAR2,
    i_comment   IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE gather_stats
  (
    i_condition         IN VARCHAR2,
    i_degree            IN PLS_INTEGER DEFAULT NULL,
    i_granularity       IN VARCHAR2 DEFAULT NULL,
    i_estimate_pct      IN NUMBER DEFAULT NULL,
    i_cascade           IN BOOLEAN DEFAULT NULL,
    i_method_opt        IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE set_stats_gather_prefs
  (
    i_pref_name   IN VARCHAR2,
    i_value       IN VARCHAR2,
    i_table_name  IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE truncate_table(i_table_name IN VARCHAR2);
 
  PROCEDURE truncate_partition(i_table_name IN VARCHAR2, i_for IN VARCHAR2);
 
  PROCEDURE truncate_subpartition(i_table_name IN VARCHAR2, i_for IN VARCHAR2);
  
  PROCEDURE add_date_subpartitions
  (
    p_table_name IN VARCHAR2,
    p_up_to      IN DATE, 
    p_period     IN VARCHAR2 DEFAULT 'YEAR', -- 'DAY','WEEK','MONTH' or 'YEAR'
    p_part_name  IN VARCHAR2 DEFAULT NULL
  );
 
  PROCEDURE drop_partition(i_table_name IN VARCHAR2, i_for IN VARCHAR2);
 
  PROCEDURE drop_subpartition(i_table_name IN VARCHAR2, i_for IN VARCHAR2);
 
  -- Procedure DROP_OLD_PARTITIONS drops all INTERVAL partitins from the given tables
  -- except the most newest ones
  PROCEDURE drop_old_partitions
  (
    i_table_list IN VARCHAR2 DEFAULT NULL,
    i_keep_newest IN PLS_INTEGER DEFAULT 2 -- how many partitions to retain
  );
 
  -- Procedure ALTER_TABLE_COLUMNS adds/modifies columns to/in the specified list of tables
  --
  -- Example:
  -- BEGIN
  --  dbm.alter_table_columns
  --  (
  --    p_table_list => 'TRN_IMP_BWE_SEC_DTL,TRN_IMP_BWE_SEC_DTL_ARCH',
  --    p_col_descriptions => 'ASSET_CLASS:VARCHAR2(255):Y, US_LCR_LIQ_LBL,VARCHAR2(255):Y'
  --  );
  -- END;
  --
  -- Note: in columns descriptions, the last attribute is "NULLABLE", 'Y' means it is nullable.
  PROCEDURE alter_table_columns(p_table_list IN VARCHAR2, p_col_descriptions IN VARCHAR2);
 
  PROCEDURE drop_tables(p_table_list IN VARCHAR2);
 
END pkg_db_maintenance;
/
 
CREATE OR REPLACE SYNONYM dbm FOR PKG_DB_MAINTENANCE;
