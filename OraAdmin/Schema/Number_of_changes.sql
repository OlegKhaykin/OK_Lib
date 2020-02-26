with
  tlist as
  (
    select --+ materialize
      owner, table_name
    from dba_tables
    where owner = 'ODS'
    and table_name = 'SUPPLIERPRODUCTRELATION'
  ),
  tsp as
  (
    select t.owner table_owner, t.table_name, null partition_name, null subpartition_name, t.num_rows, t.last_analyzed 
    from tlist                    tl
    join dba_tables               t
      on t.owner = tl.owner and t.table_name = tl.table_name
   union all 
    select t.table_owner, t.table_name, t.partition_name, null subpartition_name, t.num_rows, t.last_analyzed 
    from tlist                    tl
    join all_tab_partitions       t
      on t.table_owner = tl.owner and t.table_name = tl.table_name
   union all 
    select t.table_owner, t.table_name, t.partition_name, t.subpartition_name, t.num_rows, t.last_analyzed 
    from tlist                    tl
    join dba_tab_subpartitions    t
      on t.table_owner = tl.owner and t.table_name = tl.table_name
  )
--select * from tsp where partition_name = 'AETNA_ACCOUNT';
select
  table_owner, table_name, partition_name, subpartition_name, num_rows, last_analyzed,
  inserts, updates, deletes, change_factor,
  max(change_factor) over(partition by table_owner, table_name order by change_factor desc) max_change_factor 
from
(
  SELECT
    t.table_owner,
    t.table_name,
    t.partition_name,
    t.subpartition_name,
    t.num_rows,
    t.last_analyzed,
    m.inserts,
    m.updates,
    m.deletes,
    (NVL(m.inserts, 0) - NVL(m.deletes, 0))/CASE WHEN t.num_rows IS NULL OR t.num_rows = 0 THEN 1 ELSE t.num_rows END change_factor
  FROM tsp t
  LEFT JOIN dba_tab_modifications m
    ON m.table_owner = t.table_owner
   AND m.table_name = t.table_name
   AND NVL(m.partition_name, 'NULL') = NVL(t.partition_name, 'NULL')
   AND NVL(m.subpartition_name, 'NULL') = NVL(t.subpartition_name, 'NULL')
)
where change_factor > 0.5
order by max_change_factor desc, table_owner, table_name, partition_name nulls last, subpartition_name nulls last;

select count(1) from SUPPLIERPRODUCTRELATION;