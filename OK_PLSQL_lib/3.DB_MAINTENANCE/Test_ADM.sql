create table tst_ok_one(n number(10));
create table tst_ok_two(n number(10));

begin
  xl.open_log('TST_OK','Test');
  adm.add_columns('d:date:Y,v:varchar2(50):Y','tst_ok_one,tst_ok_two');
  xl.close_log('Success');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from user_tab_columns
where table_name like 'TST%'
order by table_name, column_id; 
