DECLARE
  cmd     VARCHAR2(200);
  cnt     pls_integer := 0;
  err_cnt pls_integer := 0;
BEGIN
  xl.open_log('DROP Amabari','Cleaning Ambari objects', TRUE);
  
  FOR cur_rec IN
  (
    SELECT object_name, object_type
    FROM user_objects
    where object_type not in ('LOB','INDEX')
    and last_ddl_time > sysdate - interval '2' hour
  )
  LOOP
    begin
      cmd := 'DROP '||cur_rec.object_type||' "'||cur_rec.object_name||'"'||CASE WHEN cur_rec.object_type = 'TABLE' THEN ' CASCADE CONSTRAINTS' END;
      xl.begin_action('Executing command', cmd);
      EXECUTE IMMEDIATE cmd; 
      xl.end_action;
      cnt := cnt+1;
    exception
     when others then
      xl.end_action(sqlerrm);
      err_cnt := err_cnt+1;
    end;
  END LOOP;
  
  if err_cnt > 0 then
    Raise_Application_Error(-20000, err_cnt||' errors happened');
  end if;
  
  xl.close_log('Successfully completed: '||cnt||' objects dropped.');
exception
 when others then
  xl.close_log(sqlerrm);
  raise;
END;
/
