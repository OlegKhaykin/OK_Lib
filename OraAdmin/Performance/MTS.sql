select name, value, isdefault
from v$parameter
where name like '%shared%' or name like '%target%' or name like '%pool%' 
or name in ('processes','sessions','dispatchers')
order by name;

alter system set dispatchers = '(protocol=tcp)(dispatchers=3)' scope=both;
alter system set shared_servers=10 scope=both;

select * from v$shared_server;
select * from v$dispatcher;
select * from v$shared_server_monitor; 


