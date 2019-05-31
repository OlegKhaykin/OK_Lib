SET VERIFY OFF
SET HEAD OFF

ACCEPT table PROMPT 'Enter tablename: '

PROMPT This table refers to:
SELECT
 c1.owner||'.'||c1.table_name||'.'||c1.constraint_name || ' -> ' ||
 c2.owner||'.'||c2.table_name||'.'||c2.constraint_name||' ('||c1.delete_rule||')'
FROM
 dba_constraints c1, dba_constraints c2
WHERE
 c1.table_name=Upper('&table') AND
 c2.constraint_name = c1.r_constraint_name AND
 c2.owner = c1.r_owner 
ORDER BY c1.r_owner, c2.table_name;

PROMPT This table is referred by:
SELECT c1.owner||'.'||c1.table_name||'.'||c1.constraint_name|| ' <- ' || c2.owner||'.'||c2.table_name||'.'||c2.constraint_name||' ('||c2.delete_rule||')'
FROM
 dba_constraints c1, dba_constraints c2
WHERE
 c1.table_name=Upper('&table') AND
 c1.constraint_name = c2.r_constraint_name AND
 c1.owner = c2.r_owner
ORDER BY c2.owner, c2.table_name, c2.constraint_name;
