SET VERIFY OFF

define obj=INCV.INCV_PROGRAMCONFIGURATION_PKG

create table N384433.tst_ok_dependencies as
SELECT * FROM dba_dependencies
WHERE owner NOT IN ('SYS','SYSTEM');

SELECT referenced_name, Lpad(' ', Lvl-1)||type||' '||owner||'.'||name||' depends on '||referenced_type||' '||referenced_owner||'.'||referenced_name explanation
from
(
  select rownum rnum, level lvl, type, owner, name, referenced_type, referenced_owner, referenced_name
  FROM n384433.tst_ok_dependencies
  CONNECT BY name = PRIOR referenced_name
  AND owner = PRIOR referenced_owner
  AND type = PRIOR referenced_type 
  START WITH owner = Upper(Substr('&obj', 1, Instr('&obj','.')-1))
  AND name = Upper(Substr('&obj', Instr('&obj','.')+1))
)
--where name = 'INCV_PROGRAMCONFIGURATION_PKG' and referenced_type = 'TABLE'
where referenced_type = 'TABLE'
order by rnum;
