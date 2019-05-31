prompt Creating procedure KILL_INACTIVE_SESSIONS

create or replace procedure kill_inactive_sessions(p_users in v2_array, p_idle_seconds in pls_integer default 1200) as
  action varchar2(128);
begin
  xl.open('Killing inactive sessions',TRUE);
  
  for r in 
  (
    select sid||','||serial# sess, spid from v_db_processes 
    where username in (select value(t) from table(cast(p_users as v2_array)) t) 
    and status = 'INACTIVE'
    and last_call_et >= p_idle_seconds
    and spid is not null
  )
  loop
    action := 'Killing session '||r.sess;
    xl.write(action, 'Started');
    execute immediate 'alter system kill session '''||r.sess||'''';
    dbms_output.put_line('kill -9 '||r.spid);
    xl.write(action, 'Done');
  end loop;
  
  xl.close('Successfully completed');
exception
 when others then 
  xl.close(sqlerrm);
  raise;
end;
/

grant execute on kill_inactive_sessions to dba;
create or replace public synonym kill_inactive_sessions for kill_inactive_sessions;
