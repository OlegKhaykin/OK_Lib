
select * from v_table_partition_info where owner = 'ODS' AND table_name like 'TST_OK%';

exec dbms_stats.gather_table_stats('ods', 'tst_ok_person', estimate_percent=>10, degree=>10, cascade=> true);
exec dbms_stats.gather_table_stats('ods', 'tst_ok_member', estimate_percent=>10, degree=>10, cascade=> true);
exec dbms_stats.gather_table_stats('ods', 'tst_ok_business_supplier', estimate_percent=>10, degree=>10, cascade=> true);

select dbms_stats.get_param('ESTIMATE_PERCENT') from dual;
select dbms_stats.get_param('DEGREE') from dual;

select DBMS_STATS.AUTO_SAMPLE_SIZE from dual;