alter session set current_schema = cdw;

select
--  owner, table_name, 
  lower(column_name)||' '||data_type||','
from v_all_columns
where owner = 'OK' AND table_name = 'TST_OK'
and nullable = 'N'
order by column_id;


select t.owner, t.table_name, t.num_rows, g.col_list
from
(
  select
    owner, table_name,
    listagg(lower(column_name)||', ') within group(order by column_id) col_list, count(1) cnt
  from v_all_columns
  where 1=1
  and owner in (/*'EPIC_CLARITY','UD_MASTER','HHC_CUSTOM',*/'CDW'/*,'PT005'*/)
  and table_name = 'FACT_VISITS'
--  and column_name like 'FIN%CLASS%'
--  and column_name IN ('VISIT_SERVICE_TYPE_ID')
  group by owner, table_name
) g
join all_tables t on t.owner = g.owner and t.table_name = g.table_name --and t.num_rows > 10 
order by col_list, table_name;

select index_name, status from user_indexes where status not in ('VALID', 'N/A')
union
select index_name, status from user_ind_partitions where status not in ('USABLE', 'N/A')
union
select index_name, status from user_ind_subpartitions where status <> 'USABLE';

SELECT * FROM user_indexes 
where 1=1
--and index_name = 'KP_ADMIT_EVENT'
and table_name = 'PROC_EVENT_ARCHIVE_GP1' 
;

select segment_name, round(sum(bytes)/1024/1024) mbytes
from user_segments
where segment_name like 'PROC_EVENT_ARCHIVE_%'
group by segment_name
ORDER BY segment_name;

select ' '''||upper(column_name)||''''
from user_tab_columns where table_name = 'DIM_PATIENTS'
order by column_id;
 