SELECT 'ALTER INDEX '||index_name||' REBUILD;'
FROM user_indexes WHERE partitioned = 'NO' AND status <> 'VALID'
ORDER BY index_name

SELECT
  'ALTER INDEX '||index_name||' REBUILD PARTITION '||partition_name||';'
FROM user_ind_partitions
WHERE status <> 'USABLE'
ORDER BY index_name
