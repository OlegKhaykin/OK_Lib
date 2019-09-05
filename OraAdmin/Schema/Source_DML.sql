select q.table_name, q.type, q.name, q.dml_operation, q.dml_line, q.table_line
from
(
  select
    sq.table_name, sq.type, sq.name, sq.table_line,
    nvl(sq.dml_operation, lag(sq.dml_operation) over(partition by sq.type, sq.name order by sq.line)) dml_operation,  
    nvl(sq.dml_line, lag(sq.dml_line) over(partition by sq.type, sq.name order by sq.line)) dml_line  
  from
  (
    select
      s.type, s.name, s.line,
      case 
        when upper(s.text) like '%INSERT%' then 'INSERT'
        when upper(s.text) like '%UPDATE%' then 'UPDATE'
        when upper(s.text) like '%DELETE%' then 'DELETE'
        when upper(s.text) like '%MERGE%'  then 'MERGE'
        when upper(s.text) like '%TRUNCATE%' then 'TRUNCATE'
      end dml_operation,
      case
       when 
        upper(s.text) like '%INSERT%'   or 
        upper(s.text) like '%UPDATE%'   or 
        upper(s.text) like '%DELETE%'   or 
        upper(s.text) like '%MERGE%'    or 
        upper(s.text) like '%TRUNCATE%' 
       then s.line 
      end dml_line, 
      t.table_name,
      nvl2(t.table_name, s.line, null) table_line
    from dba_source s
    left join dba_tables t
      on t.owner = s.owner
     and upper(s.text) like '%'||t.table_name||'%' 
     and t.table_name in ('AHMMRNBUSINESSSUPPLIER','MEMBERAGGREGATION')
    where s.owner = 'ODS'
    and
    (
      upper(s.text) like '%INSERT%'   or 
      upper(s.text) like '%UPDATE%'   or 
      upper(s.text) like '%DELETE%'   or 
      upper(s.text) like '%MERGE%'    or 
      upper(s.text) like '%TRUNCATE%' or 
      t.table_name is not null
    )
  ) sq
) q
where q.table_name is not null and q.dml_line > (q.table_line-3)
order by q.table_name, q.type, q.name, q.table_line; 