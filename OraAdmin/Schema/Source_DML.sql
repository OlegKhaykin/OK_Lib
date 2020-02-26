alter session set current_schema = AHMADMIN; 

drop table tst_ok_table_usage purge;
 
create table tst_ok_table_usage as
with
  par as
  (
    select --+ materialize
      'AHMADMIN'            AS owner,
      'PACKAGE BODY'        AS type,
      'PSA_DATA_INGEST_OK'  AS name,
      1                     AS start_line,
      15000                 AS end_line
    from dual
  ),
  src as
  (
    select --+ materialize
      line, text
    from par                                  p
    join dba_source                           s
      on s.owner = p.owner
     and s.type = p.type
     and s.name = p.name
    and text not like '%/*%'
    and text not like '%begin_action%'
    and text not like '%end_action%'
  ),
  prc as
  (
    select --+ materialize
      regexp_substr(text, '(procedure|function)', 1, 1,'i',1)                         prg_type,
      UPPER(regexp_substr(text, '(procedure|function)\s+([[:alnum:]_]+)',1,1,'i',2))  prg_name,
      line                                                                            begin_line,
      nvl(lead(line) over(order by line), 15001)-1                                    end_line, 
      text
    from src
    where regexp_like(text, 'procedure|function', 'i') 
    and text not like '%-%'
  )
--select * from prc where prg_name = 'SPT_GET_BATCHCONTROLS_AS_READY';  
  , tab as
  (
    select --+ materialize
      d.referenced_owner  as owner,
      d.referenced_type   as type,
      d.referenced_name   as table_name
    from par                                                          p
    join dba_dependencies                                             d
      on d.owner = p.owner and d.type = p.type and d.name = p.name
     and d.referenced_type in ('TABLE', 'VIEW')
  )
--select * from tab where owner = 'ODS';
  , sql_lines as
  (
    select --+ materialize
      s.name as pkg_name, s.line,
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
      t.owner                           table_owner,
      t.table_name,
      nvl2(t.table_name, s.line, null)  table_line
    from par p
    join dba_source s 
      on s.owner = p.owner and s.type = p.type and s.name = p.name
     and s.line between p.start_line and p.end_line
    left join tab t
      on regexp_like(upper(s.text), '(\.|\s|^)'||t.table_name||'(''|\s|$)') 
    where
    (
      upper(s.text) like '%INSERT%'   or 
      upper(s.text) like '%UPDATE%'   or 
      upper(s.text) like '%DELETE%'   or 
      upper(s.text) like '%MERGE%'    or 
      upper(s.text) like '%TRUNCATE%' or 
      t.table_name is not null
    )
  )
--select * from sql_lines where table_name = 'TMP_CSASUPPLIERXREF';  
  , sq as
  (
    select
      pkg_name,
      case when dml_line < table_line-1 then table_line else dml_line end sql_line,
      case when dml_line < table_line-1 then 'SELECT' else dml_operation end sql_operation,
      table_owner, table_name, table_line
    from
    (
      select
        pkg_name,
        nvl(nvl(dml_line, lag(dml_line) over(partition by pkg_name order by line)), 0) dml_line,  
        nvl(dml_operation, lag(dml_operation) over(partition by pkg_name order by line)) dml_operation,
        table_owner, table_name, table_line
      from sql_lines
    )
    where table_name is not null
  )
--select * from sq where table_name = 'PLANSPONSORCONTROLINFO';   
  , sql_info as
  (
    select
      sq.pkg_name, prc.prg_type, prc.prg_name, prc.begin_line, prc.end_line,
      sq.sql_line, sq.sql_operation, sq.table_owner, sq.table_name
    from sq
    left join prc
      on prc.begin_line <= sq.sql_line and prc.end_line >= sq.sql_line
  )
select pkg_name, prg_type, prg_name, begin_line, end_line, sql_line, sql_operation, table_owner, table_name 
from sql_info;-- where table_name = 'PLANSPONSORCONTROLINFO';

select
  prg_name, begin_line, end_line, table_owner, table_name, 
  listagg(substr(sql_operation, 1, 1), ',') within group
  (
    order by decode(substr(sql_operation, 1, 1), 'S', 1, 'I', 2, 'M', 3, 'U', 4, 5)
  ) usage 
from
(
  select distinct
    table_owner, table_name, prg_name, begin_line, end_line, sql_operation
  from tst_ok_table_usage
  where pkg_name = 'PSA_DATA_INGEST_OK'
--  and prg_name = 'SPT_CREATE_MOVE_SUPPLIER'
  and table_name IN ('STGSUPPCSADETAILS','EXPECTEDSUPPCSADETAILS') and sql_operation <> 'SELECT'
)
group by table_name, table_owner, prg_name, begin_line, end_line
order by table_name, begin_line;
