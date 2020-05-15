CREATE OR REPLACE VIEW v_index_partition_info AS
SELECT --+ USE_HASH(ISP)
/*
  This view is mainly for use by the index-handling procedures
  from the PKG_DB_MAINTENANCE package. It gives detailed information
  about each Partition/Subpartition of every locally-partitioned index.
  History of changes (newest to oldest):
  -----------------------------------------------------------------------------
  28-Jun-2016, OK: added ALL_PART_TABLES
*/
  pc.owner, pc.name AS table_name, pc.column_name AS partitioned_by, tp.high_value,
  CASE WHEN pc.column_name IN ('COB_DT', 'VAL_DT') THEN
    eval_date(tp.high_value) -  CASE pt.partitioning_type
      WHEN 'LIST' THEN INTERVAL '0' DAY
      WHEN 'RANGE' THEN INTERVAL '1' DAY
    END
  END part_dt,
  ip.index_name, ip.partition_name, ip.partition_position,
  tp.compress_for AS tab_part_compression,
  ip.compression AS part_compression,
  ip.segment_created AS part_segm_created,
  ip.status AS part_status,
  isp.subpartition_name,
  isp.compression AS subpart_compression,
  isp.segment_created AS subpart_segm_created,
  isp.status AS subpart_status
FROM all_part_key_columns pc -- assumption: all tables are partitioned by 1 column
CROSS JOIN TABLE(pkg_db_maintenance.get_partition_info(pc.owner, pc.name)) tp
JOIN all_part_tables pt ON pt.owner = pc.owner AND pt.table_name = pc.name
JOIN all_indexes i ON i.owner = pc.owner AND i.table_owner = pc.owner AND i.table_name = pc.name
JOIN all_ind_partitions ip ON ip.index_owner = pc.owner AND ip.index_name = i.index_name AND ip.partition_position = tp.partition_position
LEFT JOIN all_ind_subpartitions isp ON isp.index_owner = pc.owner AND isp.index_name = ip.index_name AND isp.partition_name = ip.partition_name;

GRANT SELECT ON v_index_partition_info TO PUBLIC;
