CREATE OR REPLACE PACKAGE pkg_db_maintenance AUTHID CURRENT_USER AS
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

  TYPE rec_column_definition IS RECORD
  (
    column_name VARCHAR2(128),
    data_type   VARCHAR2(30),
    nullable    CHAR(1) 
  );
  
  TYPE tab_column_definitions IS TABLE OF rec_column_definition;
  
  FUNCTION get_column_definitions(p_column_definitions IN VARCHAR2) RETURN tab_column_definitions PIPELINED;
  
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
  
  PROCEDURE add_columns
  (
    p_column_definitions IN VARCHAR2,
    p_table_list         IN VARCHAR2
  );
END;
/

CREATE OR REPLACE SYNONYM dbm FOR pkg_db_maintenance;
GRANT EXECUTE ON pkg_db_maintenance TO PUBLIC;
