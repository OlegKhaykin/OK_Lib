select
  c.owner||'.'||c.table_name||'.'||c.constraint_name||'('||
  concat_dataset
  (
    cursor
    (
      select column_name from dba_cons_columns
      where owner = c.owner and constraint_name = c.constraint_name
      order by position
    )
  )||') -> '||
  r.owner||'.'||r.table_name||'('||
  concat_dataset
  (
    cursor
    (
      select column_name from dba_cons_columns
      where owner = r.owner and constraint_name = r.constraint_name
      order by position
    )
  )||')'
from
  dba_constraints c,
  dba_constraints r
where c.owner = 'SAGE' and c.table_name = Upper('&1') and c.constraint_type = 'R' 
and c.r_owner = r.owner and c.r_constraint_name = r.constraint_name
