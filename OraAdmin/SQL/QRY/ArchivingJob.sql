declare v number;
begin
dbms_job.submit(v,'
  declare v number;
  begin 
    select count(*) into v from V$LOG where status=''CURRENT'' and first_time<(sysdate-20/60/24);
    if v>0 then
       execute immediate ''alter system archive log current''; 
       execute immediate ''alter system checkpoint''; 
    end if;
  end;
',sysdate+1e-5,'sysdate+20/60/24');
commit;
end;
/