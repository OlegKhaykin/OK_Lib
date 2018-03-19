CREATE OR REPLACE VIEW v_table_partition_info AS
SELECT
  t.owner, t.table_name,
  concat_v2_set
  (
    CURSOR
    (
      SELECT column_name 
      FROM all_part_key_columns
      WHERE owner = t.owner AND name = t.table_name
      ORDER BY column_position
    )
  ) AS partitioned_by,
  tp.partition_name, tp.partition_position, tp.high_value,
  NVL(tsp.tablespace_name, tp.tablespace_name) AS tablespace_name,
  tp.compress_for part_compression, tp.num_blocks part_blocks,
  tp.num_rows part_rows, tp.last_analyzed part_last_analyzed,
  tsp.subpartition_name, tsp.subpartition_position, tsp.high_value subpart_high_value,
  tsp.compress_for subpart_compression, tsp.num_blocks subpart_blocks,
  tsp.num_rows subpart_rows, tsp.last_analyzed subpart_last_analyzed
FROM all_part_tables t
CROSS JOIN TABLE(pkg_db_maintenance.get_partition_info(t.owner, t.table_name)) tp
LEFT JOIN TABLE(pkg_db_maintenance.get_subpartition_info(t.owner, t.table_name)) tsp ON tsp.partition_name = tp.partition_name;

GRANT SELECT ON v_table_partition_info TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM v_table_partition_info FOR v_table_partition_info;
