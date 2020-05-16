SELECT
  LPAD(' ', (LEVEL-1)*2)||
  c.p_owner||'.'||c.p_table||'('||c.r_constraint_name||') <- '||
  c.owner||'.'||c.table_name||'('||c.constraint_name||')'
from
(
  select
    p.owner p_owner, p.table_name p_table,  
    r.owner, r.table_name, r.constraint_name, r.r_constraint_name, 
    uk.constraint_name uk_name
  from dba_constraints r
  join dba_constraints p
    on p.owner = r.r_owner
   and p.constraint_name = r.r_constraint_name
  left join dba_constraints uk
    on uk.table_name = r.table_name
   and uk.owner = r.owner
   and uk.constraint_type in ('P','U')
  where r.owner = 'AHMADMIN'
  and r.constraint_type = 'R'
) c
connect by c.p_owner = prior c.owner and c.r_constraint_name = prior c.uk_name
start with c.p_owner = 'AHMADMIN' and c.p_table = 'PURCHASEDPRODUCT';
