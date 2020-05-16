-- This table references:
SELECT
 c1.owner||'.'||c1.table_name||'.'||c1.constraint_name || ' -> ' ||
 c2.owner||'.'||c2.table_name||'.'||c2.constraint_name||' ('||c1.delete_rule||')'
FROM dba_constraints c1
JOIN dba_constraints c2
  ON c2.constraint_name = c1.r_constraint_name
 AND c2.owner = c1.r_owner 
WHERE c1.table_name IN ('EXCLUDEDPROVIDERXREF') AND c1.owner = 'ODS' AND c1.constraint_type = 'R'
ORDER BY c1.r_owner, c2.table_name;

-- This table is referenced by:
SELECT c1.owner||'.'||c1.table_name||'.'||c1.constraint_name|| ' <- ' || c2.owner||'.'||c2.table_name||'.'||c2.constraint_name||' ('||c2.delete_rule||')'
FROM dba_constraints c1
JOIN dba_constraints c2
  ON c2.r_constraint_name = c1.constraint_name
 AND c2.r_owner = c1.owner
WHERE c1.table_name='EXCLUDEDPROVIDERXREF-' AND c1.owner = 'ODS'
ORDER BY c2.owner, c2.table_name, c2.constraint_name;
