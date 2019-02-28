SHOW CHARACTER SET like 'utf8%';
SELECT * FROM INFORMATION_SCHEMA.CHARACTER_SETS where character_set_name like 'utf8%';

show variables like 'tx_isolation';
show variables like 'autocommit';
show variables like '%character%';

set tx_isolation='READ-COMMITTED';
SET character_set_client='utf8mb4';

alter database OK character set = 'utf8mb4';

# In OK ################################################################
use OK;
create table tst_ok(n int, v varchar(30)) character set = 'utf8mb4';
select * from information_schema.tables where table_schema = 'OK';

insert into tst_ok values(1, 'One') ;
insert into tst_ok values(2, 'Два') ;
commit;

delete from tst_ok;
commit;

select * from tst_ok;


# In RUS ################################################################
use RUS;
create table tst_ok_rus(n int, v varchar(30)) character set = 'utf8mb4';
insert into tst_ok_rus values(1, 'One') ;
insert into tst_ok_rus values(2, 'Два') ;
commit;

delete from ts_tok_rus;
commit;

select * from tst_ok_rus;
