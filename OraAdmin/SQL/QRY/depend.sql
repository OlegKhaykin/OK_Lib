SET VERIFY OFF
ACCEPT obj PROMPT 'Specify the object ("schema.name"): '

TRUNCATE TABLE test;
INSERT INTO test SELECT * FROM dba_dependencies
WHERE owner NOT IN ('SYS','SYSTEM');

SELECT Lpad(' ', Level)||owner||'.'||name
FROM test
CONNECT BY PRIOR referenced_name=name
AND referenced_owner = owner
START WITH owner = Upper(Substr('&obj', 1, Instr('&obj','.')-1))
AND name = Upper(Substr('&obj', Instr('&obj','.')+1));
