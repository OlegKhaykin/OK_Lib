ALTER SESSION SET CURRENT_SCHEMA = ODS;

-- Multi-level research
with
  ref as
  (
    select distinct
      c.owner, c.table_name, r.owner r_owner, r.table_name r_table_name
    from dba_constraints          c
    left join dba_constraints     r
      on r.owner = c.r_owner and r.constraint_name = c.r_constraint_name
    where c.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
select
  lpad(' ', (level-1)*2) ||
  owner||'.'||table_name|| ' -> '||r_owner||'.'||r_table_name reference
from ref r
connect by owner = prior r_owner and table_name = prior r_table_name 
start with owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
and table_name IN 
(
  'MEMBER'
);
  
-- Single level research:  
select
  c.owner||'.'||c.table_name||'.'||c.constraint_name||'('||
  to_char(substr(concat_v2_set(cursor
  (
    select column_name from dba_cons_columns
    where owner = c.owner and constraint_name = c.constraint_name
    order by position
  )), 1, 500))||') -> '||
  r.owner||'.'||r.table_name||'('||
  to_char(substr(concat_v2_set(cursor
  (
    select column_name from dba_cons_columns
    where owner = r.owner and constraint_name = r.constraint_name
    order by position
  )), 1, 500))||')' reference
from dba_constraints    c
join dba_constraints    r
  on r.owner = c.r_owner
 and r.constraint_name = c.r_constraint_name
where c.owner = 'ODS'
and c.constraint_type = 'R' 
and c.table_name IN 
(
  'MEMBER'
)
order by 1;
