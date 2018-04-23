rem	Use:
rem		Put the examined SQL statement (with ';') into a file called
rem			target.sql
rem		Start this script - explain.sql
rem
rem		The script displays the current audit id, then
rem		the execution path, simultaneously writing the
rem		execution path to a file identified by the audit id.
rem
rem	Suggestions:
rem		Adjust termout on/off to taste
rem		Adjust pagesize to taste
rem		Adjust linesize to taste
rem		set pause on/off to taste
rem	

set pagesize 50000
set linesize 1000
set verify off

set def =
set def &

column plan		format a160	heading "Plan"

column id	 	format 999	heading "Id"
column parent_id 	format 999	heading "Par"
column position 	format 999	heading "Pos"
column object_instance 	format 999	heading "Ins"

column state_id new_value m_statement_id

select userenv('sessionid') state_id from dual;

explain plan
set statement_id = '&m_statement_id'
for
@@target

set feedback off
spool &m_statement_id

select
	id,
	parent_id,
	position,
	object_instance,
	rpad(' ',2*level) ||
	operation || ' ' ||
	decode(optimizer,null,null,
		'(' || lower(optimizer) || ') '
	)  ||
	object_type || ' ' ||
	object_owner || decode(object_name,null,'','.') ||
	object_name || ' ' ||
	decode(options,null,null,'('||lower(options)||') ') ||
	decode(search_columns, null,null,
		'(Columns ' || search_columns || ' '
	)  ||
	other_tag || ' ' ||
	decode(partition_id,null,null,
		'Pt id: ' || partition_id || ' '
	)  ||
	decode(partition_start,null,null,
		'Pt Range: ' || partition_start || ' - ' ||
		partition_stop || ' '
	) ||
	decode(cost,null,null,
		'Cost (' || cost || ',' || cardinality || ',' || bytes || ')'
	) plan
from plan_table
connect by prior id = parent_id and statement_id = '&m_statement_id'
start with id = 0 and statement_id = '&m_statement_id'
order by id;

select id, object_node, other
from plan_table
where statement_id = '&m_statement_id'
and other is not null
order by id;

rollback;
set feedback on
spool off
