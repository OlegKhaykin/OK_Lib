alter session set current_schema = ods;

-- Tables and Views:
select owner, object_name, object_type
from dba_objects
where 1=1
AND owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
--And owner = 'AHMADMIN'
and object_type in ('TABLE', 'VIEW')
order by owner, object_name;

desc dba_segments;

-- Tables:
select
  t.owner, t.table_name, t.num_rows,
  t.compression, t.compress_for,
  s.tablespace_name, count(1) parts, sum(s.blocks) blocks, round(sum(s.bytes)/1024/1024) mbytes
from dba_tables t
join dba_segments s on s.owner = t.owner and s.segment_name = t.table_name
where 1=1
and t.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and t.table_name in ('PERSON', 'MEMBER', 'AHMMRNBUSINESSSUPPLIER', 'MEMBERAGGREGATION', 'CAREENGINEMEMBERPROCESSSTATUS','TST_OK_MEMBER','TST_OK_PERSON','TST_OK_BUSINESS_SUPPLIER')
--and table_name = 'CAREPROVIDER'
--and t.compression <> 'DISABLED'
group by t.owner, t.table_name, t.num_rows, t.compression, t.compress_for, s.tablespace_name
order by t.owner, t.table_name;

-- Columns:
select
  owner, table_name, column_id, column_name, 
  CASE
    WHEN data_type IN ('CHAR', 'VARCHAR2', 'RAW', 'NCHAR', 'NVARCHAR2') THEN data_type||'('||char_length||' '||DECODE(char_used, 'B', 'BYTE', 'C', 'CHAR')||')'
    WHEN data_type = 'NUMBER' AND data_precision IS NULL AND data_scale IS NULL THEN 'NUMBER'
    WHEN data_type = 'NUMBER' AND data_precision IS NULL AND data_scale = 0 THEN 'INTEGER'
    WHEN data_type = 'NUMBER' AND data_scale = 0 THEN 'NUMBER('||data_precision||')'
    WHEN data_type = 'NUMBER' THEN 'NUMBER('||data_precision||','||data_scale||')'
    ELSE data_type
  END data_type,
  CASE nullable WHEN 'N' THEN 'NOT NULL' END nullable
from dba_tab_columns
where owner = 'ODS'
--and table_name in ('MASTERSUPPLIER')
and column_name LIKE 'CAREPROVIDERID'
order by owner, table_name, column_id;

-- Partitions:
select
  tp.table_owner, tp.table_name, tp.tablespace_name,
  tp.partition_name, tp.partition_position, tp.high_value,
  tp.compression, tp.compress_for, tp.blocks, tp.num_rows, tp.last_analyzed
from dba_tab_partitions tp
where 1=1
and tp.table_owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
--and tp.compression <> 'DISABLED'
and tp.table_name in ('PERSON', 'MEMBER', 'AHMMRNBUSINESSSUPPLIER', 'MEMBERAGGREGATION', 'CAREENGINEMEMBERPROCESSSTATUS')
order by tp.table_owner, tp.table_name, tp.partition_position;

-- Partitions and sub-partitions using my proprietary view:
select * from v_table_partition_info
where owner = 'ODS'
and table_name like 'TST_OK%';

-- Partition Keys:
select * from dba_part_key_columns
where owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and name in ('PERSON', 'MEMBER', 'AHMMRNBUSINESSSUPPLIER', 'MEMBERAGGREGATION', 'CAREENGINEMEMBERPROCESSSTATUS')
order by owner, name, column_position;

-- Indexes:
select
  i.owner, i.index_name, i.table_owner, i.table_name, i.uniqueness, i.tablespace_name,
  listagg(ic.column_name,', ') within group (order by ic.column_position) cols
from dba_indexes i
join dba_ind_columns ic on ic.index_owner = i.owner and ic.index_name = i.index_name
where i.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and i.table_name in ('PERSON', 'MEMBER', 'AHMMRNBUSINESSSUPPLIER', 'MEMBERAGGREGATION', 'CAREENGINEMEMBERPROCESSSTATUS')
group by i.owner, i.index_name, i.table_owner, i.table_name, i.uniqueness, i.tablespace_name
order by i.table_owner, i.table_name, i.index_name;

-- Constraints:
select
  c.owner, c.table_name, c.constraint_type, c.constraint_name, c.index_name, 
  rc.owner r_owner, rc.table_name r_table_name, rc.constraint_name r_constraint_name
  , listagg(cc.column_name, ', ') within group (order by position) cols
  , c.delete_rule, c.deferrable, c.deferred
from dba_constraints c
join dba_cons_columns cc on cc.owner = c.owner and cc.table_name = c.table_name and cc.constraint_name = c.constraint_name
left join dba_constraints rc on rc.owner = c.r_owner and rc.constraint_name = c.r_constraint_name
where c.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and (c.generated = 'USER NAME' or c.constraint_type <> 'C')
and c.table_name IN ('PERSON', 'MEMBER', 'AHMMRNBUSINESSSUPPLIER', 'MEMBERAGGREGATION', 'CAREENGINEMEMBERPROCESSSTATUS')
group by c.owner, c.table_name, c.constraint_type, c.constraint_name, c.index_name, rc.owner, rc.table_name, rc.constraint_name
, c.delete_rule, c.deferrable, c.deferred
order by c.owner, c.table_name, c.constraint_type, c.constraint_name;

-- Functions, Procedures and Packages:
select * 
from dba_objects 
where object_type in ('FUNCTION', 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY')
and owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and object_name = 'CEPKG_OPERATIONAL_120';

-- PL/SQL code:
select * from dba_source
where owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and lower(text) like '%sp_loadsupplierextract%'
and type <> 'PACKAGE'
order by type, name, line;