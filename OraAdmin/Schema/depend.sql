SET VERIFY OFF

define obj=CSID.CARPKG_MHSINFO_120;

create table N384433.tst_ok_dependencies as
SELECT * FROM dba_dependencies
WHERE owner NOT IN ('SYS','SYSTEM');

SELECT
  q.owner, q.referenced_name,
--  t.num_rows, t.last_analyzed
  Lpad(' ', Lvl-1)||type||' '||owner||'.'||name||' depends on '||referenced_type||' '||referenced_owner||'.'||referenced_name explanation
from
(
  select rownum rnum, level lvl, type, owner, name, referenced_type, referenced_owner, referenced_name
  from dba_dependencies
  CONNECT BY name = PRIOR referenced_name
  AND owner = PRIOR referenced_owner
  AND type = PRIOR referenced_type 
  START WITH owner = 'AHMADMIN' and type = 'PACKAGE BODY' and name = 'PSA_DATA_INGEST'
) q
--LEFT join dba_tables t on t.owner = q.owner and t.table_name = q.referenced_name
--where q.referenced_type = 'TABLE'
--where owner = 'AHMADMIN' and referenced_owner = 'ODS' and referenced_type like 'PACKAGE%'  
order by rnum;
