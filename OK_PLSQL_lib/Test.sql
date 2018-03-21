DROP TABLE tst_ok;

purge recyclebin;

create table tst_ok
(
  owner VARCHAR2(128 BYTE),
  object_name VARCHAR2(128 BYTE),
  object_id NUMBER,
  created DATE,
  last_ddl_time DATE,
  namespace NUMBER
);



begin
  xl.open_log('TST_OK','Tesing ETL', TRUE);

  etl.add_data
  (
    p_operation => 'REPLACE',
    p_tgt => 'tst_ok',
    p_src => 'all_objects',
    p_uk_col_list => 'OWNER,OBJECT_NAME',
    p_whr => 'WHERE object_type in (''TABLE'',''VIEW'')',
    p_commit_at => -1
  );
    
  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/
