select * from
(
  SELECT 
    NVL(t1.table_name, t2.table_name) table_name, 
    NVL(t1.column_name, t2.column_name) column_name, 
    t1.data_type tab1_dtype, 
    t2.data_type tab2_dtype,
    t1.column_id tab1_colid, t2.column_id tab2_colid
  FROM
  (
    SELECT table_name, column_name, data_type, column_id FROM v_all_columns
    WHERE owner = 'SYS' AND table_name = 'GV_$SESSION'
  ) t1
  FULL JOIN 
  (
    SELECT table_name, column_name, data_type, column_id FROM v_all_columns--@prod
    WHERE owner = 'SYS' AND table_name = 'V_$SESSION'
  ) t2
  ON --t2.table_name = t1.table_name AND 
  t2.column_name = t1.column_name
  WHERE 
  (
    t1.data_type IS NULL OR t2.data_type IS NULL OR t1.data_type <> t2.data_type 
  --  OR t1.column_id <> t2.column_id
  )  
)
where 1=1
--and table_name not like 'TST%' and table_name not like 'HAC%' and table_name not like 'AW$%' 
--and table_name not like 'VW%' and table_name <> 'EXCEPTIONS' and table_name <> 'PLAN_TABLE'
/*and 
  case when tab1_dtype like 'NUMBER%' then 'NUMBER' when tab1_dtype is null then 'NULL' else tab1_dtype end <> 
  case when tab2_dtype like 'NUMBER%' then 'NUMBER' when tab2_dtype is null then 'NULL' else tab2_dtype end*/  
ORDER BY table_name, column_name;

select * from dba_synonyms
where synonym_name in ('V$SESSION','GV$SESSION');