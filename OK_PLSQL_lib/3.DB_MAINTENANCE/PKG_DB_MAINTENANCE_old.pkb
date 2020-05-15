CREATE OR REPLACE PACKAGE BODY pkg_db_maintenance AS
/*
  Description: this package contains procedures for various DB maintenance tasks.
*/
  TYPE rec_short_part_descr IS RECORD
  (
    table_name          VARCHAR2(128),
    partition_name      VARCHAR2(128),
    partition_position  NUMBER(4),
    subpartition_name   VARCHAR2(128),
    part_date           DATE,
    tablespace_name     VARCHAR2(30)
  );
   
  r_descr    rec_short_part_descr;
 
  gv_sql        VARCHAR2(1000);
  mb_before     NUMBER;
  mb_after      NUMBER;
  mb_diff       NUMBER;
 
  PROCEDURE set_session_params IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  END;
 
 
  PROCEDURE reset_session_params IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DDL';
  END;
 
  
  PROCEDURE exec_sql(p_sql IN VARCHAR2) IS
  BEGIN
    xl.begin_action('Executing SQL', p_sql);
    EXECUTE IMMEDIATE p_sql;
    xl.end_action;
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    RAISE;
  END;
 
  
  FUNCTION split_string(p_string IN VARCHAR2, p_delimeter IN VARCHAR2 default ',') RETURN tab_v256 PIPELINED IS
    N_LEN       CONSTANT BINARY_INTEGER := LENGTH(p_string); 
    n_pos       BINARY_INTEGER := 1;
    n_next_pos  BINARY_INTEGER;
    v_piece     VARCHAR2(256);
  BEGIN
    WHILE n_pos < N_LEN LOOP
      n_next_pos := INSTR(p_string, p_delimeter, n_pos);
      
      IF n_next_pos = 0 THEN n_next_pos := N_LEN+1; END IF;
      v_piece := RTRIM(SUBSTR(p_string, n_pos, n_next_pos - n_pos));
      n_pos := n_next_pos + LENGTH(p_delimeter);
       
      PIPE ROW(v_piece);
    END LOOP;
  END;
  

  PROCEDURE measure_space_usage(o_par OUT NOCOPY NUMBER) IS
  BEGIN
    SELECT ROUND(SUM(bytes)/1048576) INTO o_par FROM user_segments;
  END;
 
  -- This private procedure is called from public procedures
  -- COMPRESS_DATA and GATHER_STATS to gather statistics on one table/partition
  PROCEDURE analyze_table_partition
  (
    i_table_name      IN VARCHAR2,
    i_partition_name  IN VARCHAR2 DEFAULT NULL,
    i_degree          IN PLS_INTEGER DEFAULT NULL,
    i_granularity     IN VARCHAR2 DEFAULT NULL,
    i_estimate_pct    IN NUMBER DEFAULT NULL,
    i_cascade         IN BOOLEAN DEFAULT NULL,
    i_method_opt      IN VARCHAR2 DEFAULT NULL
  ) IS
  BEGIN
    xl.begin_action('PKG_DB_MAINTENANCE_API.ANALYZE_TABLE_PARTITION', p_debug => FALSE);
   
    dbms_stats.gather_table_stats
    (
      ownname => SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
      tabname => i_table_name,
      partname => i_partition_name,
      degree => NVL(i_degree, eval_number(dbms_stats.get_prefs('DEGREE', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name))),
      granularity => NVL(i_granularity, dbms_stats.get_prefs('GRANULARITY', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name)),
      estimate_percent => NVL(i_estimate_pct, eval_number(dbms_stats.get_prefs('ESTIMATE_PERCENT', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name))),
      cascade => NVL(i_cascade, eval_boolean(dbms_stats.get_prefs('CASCADE', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name))),
      method_opt => NVL(i_method_opt, dbms_stats.get_prefs('METHOD_OPT', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name))
    );
   
    xl.end_action;
  END;       
  
  -- This private procedure is called from public procedures
  -- COMPRESS_DATA and DEALOCATE_UNUSED_SPACE to deallocate segment space above the high water mark
  PROCEDURE deallocate_unused
  (
    i_table_name        IN VARCHAR2,
    i_partition_name    IN VARCHAR2 DEFAULT NULL,
    i_subpartition_name IN VARCHAR2 DEFAULT NULL,
    i_keep              IN PLS_INTEGER DEFAULT 65536 -- 64K
  ) IS
    n_total_blocks              PLS_INTEGER;
    n_total_bytes               PLS_INTEGER;
    n_unused_blocks             PLS_INTEGER;
    n_unused_bytes              PLS_INTEGER;
    n_last_used_extent_file_id  PLS_INTEGER;
    n_last_used_extent_block_id PLS_INTEGER;
    n_last_used_block           PLS_INTEGER;
    v_segment_type              VARCHAR2(30);
    v_part_name                 VARCHAR2(30);
  BEGIN
    v_segment_type :=
    CASE
      WHEN i_subpartition_name IS NOT NULL THEN ' SUBPARTITION'
      WHEN i_partition_name IS NOT NULL THEN ' PARTITION'
    END;
   
    v_part_name := NVL(i_subpartition_name, i_partition_name);
   
    xl.begin_action('Deallocating unused space', i_table_name||v_segment_type||' '||v_part_name);
    
    DBMS_SPACE.UNUSED_SPACE
    (
      segment_owner => SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
      segment_name => i_table_name,
      partition_name => v_part_name,
      segment_type => 'TABLE'||v_segment_type,
      total_blocks => n_total_blocks,
      total_bytes => n_total_bytes,
      unused_blocks => n_unused_blocks,
      unused_bytes => n_unused_bytes,
      last_used_extent_file_id => n_last_used_extent_file_id,
      last_used_extent_block_id => n_last_used_extent_block_id,
      last_used_block => n_last_used_block
    );
   
    IF n_unused_bytes > i_keep THEN
      exec_sql
      (
        'ALTER TABLE '||i_table_name||
        CASE WHEN v_part_name IS NOT NULL THEN ' MODIFY'||v_segment_type||' '||v_part_name END ||
        ' DEALLOCATE UNUSED KEEP '||i_keep
      );
      xl.end_action;
    ELSE
      xl.end_action('Unused space is too small: '||n_unused_bytes||' bytes, skipping ...');
    END IF;
  END;
 
  
  -- This function returns a detaset that describes table [sub-]partitions
  FUNCTION get_partition_info
  (
    i_table_owner         IN VARCHAR2,
    i_table_name          IN VARCHAR2,
    i_partition_name      IN VARCHAR2 DEFAULT NULL,
    i_partition_position  IN NUMBER DEFAULT NULL
  ) RETURN tab_partition_info PIPELINED IS
    rec                   rec_partition_info;
  BEGIN
    FOR r IN
    (
      SELECT
        table_owner, table_name, tablespace_name,
        partition_name, partition_position, high_value,
        compress_for, blocks AS num_blocks, num_rows, last_analyzed
      FROM all_tab_partitions
      WHERE table_owner = i_table_owner AND table_name = i_table_name
      AND partition_name = NVL(i_partition_name, partition_name)
      AND partition_position = NVL(i_partition_position, partition_position)
    )
    LOOP
      rec.table_owner := r.table_owner;
      rec.table_name := r.table_name;
      rec.tablespace_name := r.tablespace_name;
      rec.partition_name := r.partition_name;
      rec.partition_position := r.partition_position;
      rec.high_value := r.high_value; -- LONG -> VARCHAR2
      rec.compress_for := r.compress_for;
      rec.num_blocks := r.num_blocks;
      rec.num_rows := r.num_rows;
      rec.last_analyzed := r.last_analyzed;
     
      PIPE ROW(rec);
    END LOOP;
  END;
 
  FUNCTION get_subpartition_info
  (
    i_table_owner         IN VARCHAR2,
    i_table_name          IN VARCHAR2,
    i_partition_name      IN VARCHAR2 DEFAULT NULL
  ) RETURN tab_subpartition_info PIPELINED IS
    rec rec_subpartition_info;
  BEGIN
    FOR r IN
    (
      SELECT
        table_owner, table_name, tablespace_name,
        partition_name, subpartition_name, subpartition_position, high_value,
        compress_for, blocks AS num_blocks, num_rows, last_analyzed
      FROM all_tab_subpartitions
      WHERE table_owner = i_table_owner AND table_name = i_table_name
      AND partition_name = NVL(i_partition_name, partition_name)
    )
    LOOP
      rec.table_owner := r.table_owner;
      rec.table_name := r.table_name;
      rec.tablespace_name := r.tablespace_name;
      rec.partition_name := r.partition_name;
      rec.subpartition_name := r.subpartition_name;
      rec.subpartition_position := r.subpartition_position;
      rec.high_value := r.high_value; -- LONG -> VARCHAR2
      rec.compress_for := r.compress_for;
      rec.num_blocks := r.num_blocks;
      rec.num_rows := r.num_rows;
      rec.last_analyzed := r.last_analyzed;
     
      PIPE ROW(rec);
    END LOOP;
  END;

  PROCEDURE prepare_qry(i_condition IN VARCHAR2, i_no_sub_partitions IN CHAR DEFAULT 'N') IS
  BEGIN
    xl.begin_action('Preparing query', 'Started', FALSE);
    gv_sql := CASE
    WHEN UPPER(i_condition) LIKE '%PART_DT%'
      OR UPPER(i_condition) LIKE '%PARTITION_NAME%'
      OR UPPER(i_condition) LIKE '%PART_LAST_ANALYZED%' THEN
     'SELECT table_name, partition_name, partition_position, '||
      CASE i_no_sub_partitions WHEN 'Y' THEN 'NULL AS ' END || 'subpartition_name, part_dt, tablespace_name
      FROM v_table_partition_info '|| i_condition || '
      AND owner = '''||SYS_CONTEXT('USERENV','CURRENT_SCHEMA')||'''
      ORDER BY part_dt, table_name, partition_position, subpartition_name'
    ELSE
     'SELECT
        table_name, NULL AS partition_name, 0 AS partition_position,
        NULL AS subpartition_name, TRUNC(SYSDATE) AS part_dt, tablespace_name
      FROM user_tables '|| i_condition||'
      ORDER BY table_name'
    END;
    xl.end_action(gv_sql);
  END;
 
  
  -- This is a public procedure to compress data
  PROCEDURE compress_data
  (
    i_condition         IN VARCHAR2,-- for example: 'WHERE table_name=''PATHSENS'' AND part_dt BETWEEN ''01-JAN-13'' AND ''31-DEC-13'';
    i_compress_type     IN VARCHAR2, -- for example: 'NOCOMPRESS', 'BASIC', 'OLTP', 'QUERY HIGH', etc.
    i_max_days          IN PLS_INTEGER DEFAULT NULL,
    i_update_indexes    IN CHAR DEFAULT 'N',
    i_gather_stats      IN CHAR DEFAULT 'N',
    i_deallocate_unused IN CHAR DEFAULT 'Y',
    i_tablespace        IN VARCHAR2 DEFAULT NULL
  ) IS
    rcur            SYS_REFCURSOR;
    d_day_one       DATE;
    v_table_name    VARCHAR2(30) := 'N/A';
    v_part_name     VARCHAR2(30) := 'N/A';
    v_full_name     VARCHAR2(100);
    v_compress      VARCHAR2(30);
    v_update_idx    VARCHAR2(30);
    v_tablespace    VARCHAR2(50);
   
    PROCEDURE gather_stat IS
    BEGIN
      IF i_gather_stats = 'Y' AND v_table_name <> 'N/A' THEN
        xl.begin_action('Analyzing table '||v_table_name||' partition '||v_part_name);
        analyze_table_partition(v_table_name, v_part_name);
        xl.end_action;
      END IF;
    END;
   
  BEGIN
    xl.open_log('COMPRESS_DATA', 'Condition: '||i_condition||'; Compress type: '||i_compress_type||'; Update Indexes: '||i_update_indexes||'; Gather stats: '||i_gather_stats, TRUE);
 
    set_session_params;
   
    measure_space_usage(mb_before);
   
    v_compress := CASE i_compress_type
      WHEN 'NOCOMPRESS' THEN ' NOCOMPRESS'
      WHEN 'BASIC' THEN ' COMPRESS BASIC'
      ELSE ' COMPRESS FOR '||i_compress_type
    END;
   
    v_tablespace := CASE WHEN i_tablespace IS NOT NULL THEN ' TABLESPACE '||i_tablespace END;
   
    v_update_idx := CASE i_update_indexes WHEN 'Y' THEN ' UPDATE INDEXES' END;
   
    prepare_qry(i_condition);
   
    xl.begin_action('Opening cursor for', gv_sql);
    OPEN rcur FOR gv_sql;
    xl.end_action;
 
    LOOP
      FETCH rcur INTO r_descr;
      EXIT WHEN rcur%NOTFOUND;
     
      IF r_descr.table_name <> v_table_name OR r_descr.partition_name <> v_part_name THEN
        IF v_table_name = 'N/A' THEN
          d_day_one := r_descr.part_date;
        END IF;
       
        gather_stat;
       
        IF r_descr.part_date - d_day_one > i_max_days THEN
          EXIT;
        END IF;
       
        v_table_name := r_descr.table_name;
        v_part_name := r_descr.partition_name;
       
        CASE WHEN r_descr.partition_name IS NULL THEN
          exec_sql
          (
            'ALTER TABLE '||v_table_name||' MOVE'||v_compress||v_tablespace||v_update_idx||' PARALLEL 16'
          );
          IF i_deallocate_unused = 'Y' THEN
            deallocate_unused(v_table_name);
          END IF;
         
        WHEN r_descr.subpartition_name IS NULL THEN
          exec_sql
          (
            'ALTER TABLE '||v_table_name||' MOVE PARTITION '||v_part_name||
            v_compress||v_tablespace||v_update_idx||' PARALLEL 16'
          );
         
          IF i_deallocate_unused = 'Y' THEN
            deallocate_unused(v_table_name, v_part_name);
          END IF;
        ELSE
          exec_sql('ALTER TABLE '||v_table_name||' MODIFY PARTITION '||v_part_name||v_compress);
        END CASE;
      END IF;
     
      IF r_descr.subpartition_name IS NOT NULL THEN
        exec_sql
        (
          'ALTER TABLE '||v_table_name||' MOVE SUBPARTITION '||r_descr.subpartition_name||
          v_compress||v_tablespace||v_update_idx||' PARALLEL 16'
        );
       
        IF i_deallocate_unused = 'Y' THEN
          deallocate_unused(v_table_name, v_part_name, r_descr.subpartition_name);
        END IF;
      END IF;
    END LOOP;
   
    CLOSE rcur;
   
    gather_stat;
   
    measure_space_usage(mb_after);
 
    mb_diff := mb_after - mb_before;
   
    v_full_name := CASE WHEN mb_diff <= 0 THEN 'Freed' ELSE 'Used more' END ||' space: '||ABS(mb_diff)||' MB';
   
    reset_session_params;
   
    xl.close_log(v_full_name);
  EXCEPTION
   WHEN OTHERS THEN
    xl.close_log(SQLERRM, TRUE);
    reset_session_params;
    RAISE;
  END compress_data;
 
  
  PROCEDURE deallocate_unused_space -- to reclaim unused disk space
  (
    i_condition       IN VARCHAR2 -- for example: 'WHERE table_name=''PATHSENS'' AND RevalDate BETWEEN ''01-JAN-13'' AND ''31-DEC-13''';
  ) IS
    rcur          SYS_REFCURSOR;
  BEGIN
    xl.open_log('DEALLOCATE_UNUSED_SPACE', 'Condition: '||i_condition);
    xl.begin_action('Deallocating unused space', 'Condition: '||i_condition);
 
    set_session_params;
   
    measure_space_usage(mb_before);
   
    prepare_qry(i_condition);
 
    xl.begin_action('Opening cursor for', gv_sql);
    OPEN rcur FOR gv_sql;
    xl.end_action;
   
    LOOP
      FETCH rcur INTO r_descr;
      EXIT WHEN rcur%NOTFOUND;
     
      CASE WHEN r_descr.partition_name IS NULL THEN
        deallocate_unused(r_descr.table_name);
      WHEN r_descr.subpartition_name IS NULL THEN
        deallocate_unused(r_descr.table_name, r_descr.partition_name);
      ELSE
        deallocate_unused(r_descr.table_name, r_descr.partition_name, r_descr.subpartition_name);
      END CASE;
    END LOOP;
   
    CLOSE rcur;
   
    measure_space_usage(mb_after);
   
    mb_diff := mb_after - mb_before;
       
    xl.end_action(CASE WHEN mb_diff <= 0 THEN 'Freed' ELSE 'Used more' END ||' space: '||ABS(mb_diff)||' MB');
   
    reset_session_params;
   
    xl.close_log('Success');
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    xl.close_log('Failure');
    reset_session_params;
    RAISE;
  END;
 
 
  -- Procedure to re-create indexes using "COMPRESS" option.
  PROCEDURE compress_indexes
  (
    i_table_list      IN VARCHAR2 DEFAULT NULL, -- comma-separated list of tables
    i_tablespace_name IN VARCHAR2,
    i_force           IN CHAR DEFAULT 'N' -- if 'Y' then the indexes will be rebuilt even if their compression is already enabled
  ) IS
    no_such_index EXCEPTION;
    PRAGMA EXCEPTION_INIT(no_such_index, -1418);
   
  BEGIN
    xl.open_log('COMPRESS_INDEXES', 'One-time manual process');
   
    set_session_params;
   
    xl.begin_action('Compressing indexes', CASE WHEN i_table_list IS NOT NULL THEN 'Tables: '||i_table_list END);
   
    measure_space_usage(mb_before);
   
    FOR r IN
    (
      SELECT
        t.table_name, i.index_name, i.uniqueness,
        c.constraint_name, c.constraint_type,
        i.compression, ic.idx_col_list, ic.idx_col_cnt
      FROM user_tables t  -- it is OK to use USER_* dictionary views;
      JOIN user_indexes i -- they show objects owned by the User whose authorization is in effect
        ON i.table_name = t.table_name
      JOIN
      (
        SELECT
          table_name, index_name,
          LISTAGG(ic.column_name, ',') WITHIN GROUP(ORDER BY ic.column_position) idx_col_list,
          COUNT(1) idx_col_cnt
        FROM user_ind_columns ic
        GROUP by table_name, index_name
      ) ic
      ON ic.table_name = t.table_name and ic.index_name = i.index_name
      LEFT JOIN user_constraints c
        ON c.table_name = i.table_name AND c.index_name = i.index_name
      WHERE t.partitioned = 'YES' AND
      (
        i_table_list IS NULL
        OR t.table_name IN (SELECT COLUMN_VALUE FROM TABLE(split_string(i_table_list)))
      )
      AND (i.compression = 'DISABLED' OR i_force = 'Y')
      AND (i.uniqueness = 'NONUNIQUE' OR ic.idx_col_cnt > 1)
      AND ic.idx_col_list NOT LIKE '%SYS%$' -- no function-based indexes
      ORDER BY t.table_name, i.index_name
    )
    LOOP
      xl.begin_action('Compressing index', r.index_name);
     
      IF r.constraint_name IS NOT NULL THEN
        exec_sql('ALTER TABLE '||r.table_name||' DISABLE CONSTRAINT '||r.constraint_name);
      END IF;
     
      BEGIN
        exec_sql('DROP INDEX '||r.index_name);
       
        exec_sql
        (
          'CREATE'||CASE r.uniqueness WHEN 'UNIQUE' THEN ' UNIQUE' END ||' INDEX '||
          r.index_name||' ON '||r.table_name||'('||r.idx_col_list||')
          TABLESPACE '||i_tablespace_name||' LOCAL COMPRESS PARALLEL 32 '
        );
       
        IF r.constraint_name IS NOT NULL THEN
          exec_sql('ALTER TABLE '||r.table_name||' ENABLE CONSTRAINT '||r.constraint_name);
        END IF;
       
      EXCEPTION
       WHEN no_such_index THEN
        exec_sql('ALTER TABLE '||r.table_name||' DROP CONSTRAINT '||r.constraint_name);
       
        exec_sql
        (
          'CREATE'||CASE r.uniqueness WHEN 'UNIQUE' THEN ' UNIQUE' END ||' INDEX '||
          r.index_name||' ON '||r.table_name||'('||r.idx_col_list||')
          TABLESPACE '||i_tablespace_name||' LOCAL COMPRESS PARALLEL 32'
        );
       
        exec_sql
        (
          'ALTER TABLE '||r.table_name||' ADD CONSTRAINT '||r.constraint_name ||
          CASE r.constraint_type WHEN 'P' THEN ' PRIMARY KEY' ELSE ' UNIQUE' END ||
          '('||r.idx_col_list||') USING INDEX '||r.index_name
        );
      END;
     
      exec_sql('ALTER INDEX '||r.index_name||' NOPARALLEL');
     
      xl.end_action;
    END LOOP;
   
    measure_space_usage(mb_after);
   
    mb_diff := mb_after - mb_before;
   
    xl.end_action(CASE WHEN mb_diff <= 0 THEN 'Freed' ELSE 'Used more' END ||' space: '||ABS(mb_diff)||' MB');
   
    reset_session_params;
 
    xl.close_log('Success');
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    xl.close_log('Failure');
    reset_session_params;
    RAISE;
  END compress_indexes;
 
  
  -- This public procedure marks Index Partitions as "UNUSABLE"
  -- making Oracle remove the corresponding segments from the database
  PROCEDURE disable_index_partitions
  (
    i_condition IN VARCHAR2,
    i_comment   IN VARCHAR2 DEFAULT NULL
  ) IS
    rcur          SYS_REFCURSOR;
    rec           v_index_partition_info%ROWTYPE;
  BEGIN
    xl.open_log('DISABLE_INDEX_PARTITIONS', NVL(i_comment, i_condition));
   
    set_session_params;
   
    xl.begin_action('Disabling index partitions/subpartitions', i_condition);
   
    measure_space_usage(mb_before);
   
    gv_sql := 'SELECT * FROM v_index_partition_info '||i_condition||'
    AND owner = '''||SYS_CONTEXT('USERENV','CURRENT_SCHEMA')||'''
    AND (subpart_segm_created = ''YES'' OR part_segm_created = ''YES'')
    ORDER BY revaldate, table_name, index_name, partition_name, subpartition_name';
   
    xl.begin_action('Opening cursor for', gv_sql);
    OPEN rcur FOR gv_sql;
    xl.end_action;
   
    LOOP
      FETCH rcur INTO rec;
      EXIT WHEN rcur%NOTFOUND;
     
      exec_sql
      (
        'ALTER INDEX '||rec.index_name||' MODIFY '||
        CASE WHEN rec.subpartition_name IS NOT NULL THEN 'SUB' END || 'PARTITION '||
        NVL(rec.subpartition_name, rec.partition_name) || ' UNUSABLE'
      );
    END LOOP;
    CLOSE rcur;
   
    measure_space_usage(mb_after);
   
    mb_diff := mb_after - mb_before;
   
    xl.end_action(CASE WHEN mb_diff <= 0 THEN 'Freed' ELSE 'Used more' END ||' space: '||ABS(mb_diff)||' MB');
   
    reset_session_params;
   
    xl.close_log('Success');
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    xl.close_log('Failure');
    reset_session_params;
    RAISE;
  END disable_index_partitions;
 
 
  -- This public procedure rebuilds Index Partitions/Sub-partitions
  PROCEDURE enable_index_partitions
  (
    i_condition IN VARCHAR2,
    i_comment   IN VARCHAR2 DEFAULT NULL
  ) IS
    rcur          SYS_REFCURSOR;
    rec           v_index_partition_info%ROWTYPE;
  BEGIN
    xl.open_log('ENABLE_INDEX_PARTITIONS', NVL(i_comment, i_condition));
   
    set_session_params;
    measure_space_usage(mb_before);
   
    xl.begin_action('Enabling index partitions/subpartitions', i_condition);
   
    gv_sql := 'SELECT * FROM v_index_partition_info '||i_condition||'
    AND owner = '''||SYS_CONTEXT('USERENV','CURRENT_SCHEMA')||'''
    AND
    (
      subpartition_name IS NOT NULL AND subpart_segm_created = ''NO''
      OR
      subpartition_name IS NULL AND part_segm_created = ''NO''
    )
    ORDER BY revaldate, table_name, index_name, partition_name, subpartition_name';
   
    xl.begin_action('Opening cursor for', gv_sql);
    OPEN rcur FOR gv_sql;
    xl.end_action;
   
    LOOP
      FETCH rcur INTO rec;
      EXIT WHEN rcur%NOTFOUND;
     
      exec_sql
      (
        'ALTER INDEX '||rec.index_name||' REBUILD '||
        CASE WHEN rec.subpartition_name IS NOT NULL THEN 'SUB' END || 'PARTITION '||
        NVL(rec.subpartition_name, rec.partition_name)
      );
    END LOOP;
   
    CLOSE rcur;
 
    measure_space_usage(mb_after);
   
    mb_diff := mb_after - mb_before;
   
    xl.end_action(CASE WHEN mb_diff <= 0 THEN 'Freed' ELSE 'Used more' END ||' space: '||ABS(mb_diff)||' MB');
   
    reset_session_params;
   
    xl.close_log('Success');
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(SQLERRM);
    xl.close_log('Failure');
    reset_session_params;
    RAISE;
  END enable_index_partitions;
 
  
  -- This public procedure gathers statistics on Tables, their Partitions
  -- and Subpartitions
  PROCEDURE gather_stats
  (
    i_condition         IN VARCHAR2,
    i_degree            IN PLS_INTEGER DEFAULT NULL,
    i_granularity       IN VARCHAR2 DEFAULT NULL,
    i_estimate_pct      IN NUMBER DEFAULT NULL,
    i_cascade           IN BOOLEAN DEFAULT NULL,
    i_method_opt        IN VARCHAR2 DEFAULT NULL
  ) IS
    rcur          SYS_REFCURSOR;
    v_full_name   VARCHAR2(100);
    v_lock        VARCHAR2(128);
    n_locked      PLS_INTEGER;
    ts_start      TIMESTAMP;
    ts_last       TIMESTAMP;
  BEGIN
    ts_start := SYSTIMESTAMP;
   
    xl.open_log('GATHER_STATS', i_condition, TRUE);
/*   
    prepare_qry(i_condition, 'Y');
   
    gv_sql := REPLACE(gv_sql, 'SELECT', 'SELECT DISTINCT');
   
    xl.begin_action('Opening cursor for query', gv_sql, FALSE);
    OPEN rcur FOR gv_sql;
    xl.end_action;
    
    LOOP
      FETCH rcur INTO r_descr;
      EXIT WHEN rcur%NOTFOUND;
     
      v_full_name := r_descr.table_name || CASE WHEN r_descr.partition_name IS NOT NULL THEN ' PARTITION('||r_descr.partition_name||')' END;
      
      xl.begin_action('Analyzing table/partition', v_full_name);
     
      DBMS_LOCK.ALLOCATE_UNIQUE(v_full_name, v_lock);
      n_locked := DBMS_LOCK.REQUEST(lockhandle => v_lock, timeout => 0);
       
      IF n_locked = 0 THEN -- Successfully locked
        IF r_descr.partition_name IS NOT NULL THEN
          xl.begin_action('Finding "Last Analyzed" attribute for the partition', r_descr.partition_name, FALSE);
          SELECT DISTINCT part_last_analyzed INTO ts_last
          FROM v_table_partition_info
          WHERE owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
          AND table_name = r_descr.table_name AND partition_name = r_descr.partition_name;
          xl.end_action;
        ELSE
          xl.begin_action('Finding "Last Analyzed" attribute for the table', r_descr.table_name, FALSE);
          SELECT last_analyzed INTO ts_last
          FROM user_tables WHERE table_name = r_descr.table_name;
          xl.end_action;
        END IF;
         
        IF ts_last > ts_start THEN
          xl.end_action('This table/partition has been already analyzed, skipping ...');
        ELSE
          analyze_table_partition
          (
            i_table_name => r_descr.table_name,
            i_partition_name => r_descr.partition_name,
            i_degree => i_degree,
            i_granularity => i_granularity,
            i_estimate_pct => i_estimate_pct,
            i_cascade => i_cascade,
            i_method_opt => i_method_opt
          );
          xl.end_action;
        END IF;
       
        n_locked := DBMS_LOCK.RELEASE(v_lock);
        IF n_locked > 0 THEN
          xl.begin_action('Releasing lock on '||v_full_name);
          Raise_Application_Error(-20000, 'DBMS_LOCK.RELEASE returned unexpected result: '||n_locked);
        END IF;
       
      ELSIF n_locked = 1 THEN
        xl.end_action('Another session has locked this table/partition, skipping ...');
      ELSE
        Raise_Application_Error(-20000, 'DBMS_LOCK.REQUEST returned unexpected result: '||n_locked);
      END IF;
    END LOOP;
*/   
    xl.close_log('Success');
  EXCEPTION
   WHEN OTHERS THEN
    xl.end_action(DBMS_UTILITY.FORMAT_ERROR_STACK);
/*   
    IF v_lock IS NOT NULL THEN
      BEGIN
        xl.begin_action('Trying to release the lock');
        n_locked := DBMS_LOCK.RELEASE(v_lock);
        xl.end_action;
      EXCEPTION
       WHEN OTHERS THEN
        xl.end_action(SQLERRM);
      END;
    END IF;
*/   
    xl.close_log('Failure', TRUE);
   
    RAISE;
  END;
 
  
  PROCEDURE set_stats_gather_prefs
  (
    i_pref_name   IN VARCHAR2,
    i_value       IN VARCHAR2,
    i_table_name  IN VARCHAR2 DEFAULT NULL
  ) IS
  BEGIN
    IF i_table_name IS NULL THEN
      dbms_stats.set_schema_prefs(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_pref_name, i_value);
    ELSE
      dbms_stats.set_table_prefs(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), i_table_name, i_pref_name, i_value);
    END IF;
  END;
 
  
  PROCEDURE truncate_table(i_table_name IN VARCHAR2) IS
    v_sql VARCHAR2(256);
  BEGIN
    exec_sql('TRUNCATE TABLE '||i_table_name);
  END;
 
  
  PROCEDURE truncate_partition
  (
    i_table_name    IN VARCHAR2,
    i_for           IN VARCHAR2
  ) IS
  BEGIN
    exec_sql('ALTER TABLE '||i_table_name||' TRUNCATE PARTITION FOR ('||i_for||')');
  EXCEPTION
   WHEN OTHERS THEN
    IF SQLCODE IN (-2149, -14702) THEN -- partition does not exist
      NULL;
    ELSE
      RAISE;
    END IF;
  END;
 
  
  PROCEDURE truncate_subpartition
  (
    i_table_name    IN VARCHAR2,
    i_for           IN VARCHAR2
  ) IS
  BEGIN
    exec_sql('ALTER TABLE '||i_table_name||' TRUNCATE SUBPARTITION FOR ('||i_for||')');
  EXCEPTION
   WHEN OTHERS THEN
    IF SQLCODE IN (-2149, -14702) THEN -- partition does not exist
      NULL;
    ELSE
      RAISE;
    END IF;
  END;
 
  
  PROCEDURE add_date_subpartitions
  (
    p_table_name IN VARCHAR2,
    p_up_to      IN DATE, 
    p_period     IN VARCHAR2 DEFAULT 'YEAR', -- 'DAY','WEEK','MONTH' or 'YEAR'
    p_part_name  IN VARCHAR2 DEFAULT NULL
  ) IS
    d_high_lim   DATE;
    d_next_lim   DATE;
  BEGIN
    xl.open_log('OK-add part','Adding subpartitions to '||p_table_name, TRUE);
    
    IF p_period NOT IN ('DAY','WEEK','MONTH','YEAR') THEN
      Raise_Application_Error(-20000, 'Wrong P_PERIOD: '''||p_period||'''. Should be ''DAY'',''WEEK'',''MONTH'' or ''YEAR''.');
    END IF;
    
    FOR r IN
    (
      SELECT partition_name
      FROM all_tab_partitions
      WHERE table_name = p_table_name
      AND partition_name = NVL(p_part_name, partition_name)
      ORDER BY partition_position
    )
    LOOP
      xl.begin_action('Processing partition '||r.partition_name);
      
      SELECT MAX(eval_date(subpart_high_value)) INTO d_high_lim
      FROM v_table_partition_info
      WHERE table_name = p_table_name AND partition_name = r.partition_name;
     
      WHILE d_high_lim < p_up_to LOOP
        CASE p_period
          WHEN 'DAY' THEN d_next_lim := d_high_lim + 1;
          WHEN 'WEEK' THEN d_next_lim := d_high_lim + 7;
          WHEN 'MONTH' THEN d_next_lim := ADD_MONTHS(d_high_lim, 1);
          WHEN 'YEAR'  THEN d_next_lim := ADD_MONTHS(d_high_lim, 12);
        END CASE;
      
        exec_sql
        (
          'ALTER TABLE '||p_table_name||' MODIFY PARTITION '||r.partition_name||
          ' ADD SUBPARTITION '||r.partition_name||
          '_'||TO_CHAR(d_high_lim, CASE p_period WHEN 'YEAR' THEN 'YYYY' WHEN 'MONTH' THEN 'YYYY_MON' ELSE 'YYYYMMDD' END)||
          ' VALUES LESS THAN (DATE '''||TO_CHAR(d_next_lim, 'YYYY-MM-DD')||''')'
        );
        
        d_high_lim := d_next_lim;
      END LOOP;
      
      xl.end_action;
    END LOOP;
    
    xl.close_log('Successfully completed');
  EXCEPTION
   WHEN OTHERS THEN
    xl.close_log(SQLERRM, TRUE);
    RAISE;
  END;
  
  
  PROCEDURE drop_partition
  (
    i_table_name    IN VARCHAR2,
    i_for           IN VARCHAR2
  ) IS
  BEGIN
    exec_sql('ALTER TABLE '||i_table_name||' DROP PARTITION FOR ('||i_for||')');
  EXCEPTION
   WHEN OTHERS THEN
    IF SQLCODE IN (-2149, -14702) THEN -- partition does not exist
      NULL;
    ELSE
      RAISE;
    END IF;
  END;
 
  
  PROCEDURE drop_subpartition
  (
    i_table_name    IN VARCHAR2,
    i_for           IN VARCHAR2
  ) IS
  BEGIN
    exec_sql('ALTER TABLE '||i_table_name||' DROP SUBPARTITION FOR ('||i_for||')');
  EXCEPTION
   WHEN OTHERS THEN
    IF SQLCODE IN (-2149, -14702) THEN -- partition does not exist
      NULL;
    ELSE
      RAISE;
    END IF;
  END;
 
  
  -- Procedure DROP_OLD_PARTTIONS drops all INTERVAL partitins from the given tables
  -- except the newest ones
  PROCEDURE drop_old_partitions(i_table_list IN VARCHAR2 DEFAULT NULL, i_keep_newest IN PLS_INTEGER DEFAULT 2) IS
  BEGIN
    FOR r IN
    (
      SELECT * FROM
      (
        SELECT
          tp.table_name,
          tp.partition_name,
          tp.partition_position,
          RANK() OVER(PARTITION BY tp.table_name ORDER BY tp.partition_position DESC) rnk
        FROM TABLE(split_string(i_table_list)) t
        JOIN v_table_partition_info tp ON tp.table_name = VALUE(t)
         AND tp.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
      )
      WHERE rnk > i_keep_newest
      ORDER BY DECODE(table_name, 'DBG_PROCESS_LOGS', 2, 1), table_name, partition_position
    )
    LOOP
      xl.begin_action('Dropping partition '||r.table_name||'('||r.partition_name||')');
      EXECUTE IMMEDIATE 'ALTER TABLE '||r.table_name||' DROP PARTITION '||r.partition_name;
      xl.end_action;
    END LOOP;
  END;
 
  -- Procedure ALTER_TABLE_COLUMNS adds columns to or modifies columns in
  -- the specified list of tables.
  --
  -- Example:
  -- -------------------------------------------------------------------
  -- BEGIN
  --  dbm.alter_table_columns
  --  (
  --   p_table_list => 'TRN_IMP_BWE_SEC_DTL,TRN_IMP_BWE_SEC_DTL_ARCH',
  --   p_col_descriptions => 'ASSET_CLASS:VARCHAR2(255):Y, US_LCR_LIQ_LBL,VARCHAR2(255):Y'
  --  );
  -- END;
  -- Note: in columns descriptions, the last attribute is "NULLABLE", 'Y' means it is nullable.
  PROCEDURE alter_table_columns(p_table_list IN VARCHAR2, p_col_descriptions IN VARCHAR2) IS
  BEGIN
    FOR r IN
    (
      SELECT
         t.COLUMN_VALUE AS table_name, c.*,
         NVL2(tc.table_name, 'Y', 'N') col_exists,
         DECODE(tc.data_type, c.data_type, 'Y', 'N') data_type_matches,
         DECODE(tc.nullable, c.nullable, 'Y', 'N') nullable_matches
      FROM TABLE(split_string(UPPER(TRANSLATE(p_table_list, '. '||CHR(9)||CHR(10)||CHR(12),'.')))) t
      CROSS JOIN
      (
        SELECT *
        FROM
        (
          SELECT
            TRUNC((ROWNUM-1)/3)+1 AS col_id, MOD(ROWNUM, 3) AS attr_num, attr.COLUMN_VALUE AS attr_val
          FROM TABLE(split_string(UPPER(TRANSLATE(p_col_descriptions, '. '||CHR(9)||CHR(10)||CHR(12),'.')))) cols
          CROSS JOIN TABLE(split_string(REPLACE(cols.COLUMN_VALUE,':',','))) attr
        )
        PIVOT(MAX(attr_val) FOR attr_num IN (1 AS column_name, 2 AS data_type, 0 AS nullable))
      ) c
      LEFT JOIN v_all_columns tc
        ON tc.owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
       AND tc.table_name = t.COLUMN_VALUE AND tc.column_name = c.column_name
      ORDER BY table_name, col_id
    )
    LOOP
      xl.begin_action('Next line', 'TABLE_NAME: '||r.table_name||'; COLUMN_NAME: '||r.column_name||'; DATA_TYPE: '||r.data_type||'; NULLABLE: '||r.nullable);
      xl.end_action('COL_EXISTS: '||r.col_exists||'; DATA_TYPE_MATCHES: '||r.data_type_matches||'; NULLABLE_MATCHES: '||r.nullable_matches);
      IF r.col_exists = 'N' THEN
        exec_sql('ALTER TABLE '||r.table_name||' ADD '||r.column_name||' '||r.data_type||CASE r.nullable WHEN 'N' THEN ' NOT NULL' END);
      ELSIF r.data_type_matches = 'N' OR r.nullable_matches = 'N' THEN
        exec_sql('ALTER TABLE '||r.table_name||' MODIFY '||r.column_name||' '||r.data_type||CASE WHEN r.nullable_matches = 'N' THEN CASE r.nullable WHEN 'N' THEN ' NOT NULL' ELSE ' NULL' END END);
      END IF;
    END LOOP;
  END;
 
  
  PROCEDURE drop_tables(p_table_list IN VARCHAR2) IS
  BEGIN
    FOR r IN
    (
      SELECT t.table_name
      FROM TABLE(split_string(UPPER(TRANSLATE(p_table_list, '. '||CHR(10)||CHR(12)||CHR(9), '.')))) tl
      JOIN all_tables t ON t.table_name = tl.COLUMN_VALUE AND t.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
    )
    LOOP
      exec_sql('DROP TABLE '||r.table_name||' CASCADE CONSTRAINTS PURGE');
    END LOOP;
  END;
 END pkg_db_maintenance;
/
