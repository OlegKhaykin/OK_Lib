select
  r.owner||'.'||r.table_name||' <- '||c.owner||'.'||c.table_name||'.'||c.constraint_name
from
  dba_constraints r,
  dba_constraints c
where r.owner = 'SAGE' and r.table_name = Upper('&1')
and c.r_owner = r.owner and c.constraint_type = 'R' and c.r_constraint_name = r.constraint_name
