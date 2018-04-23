select * from rc_database;

select concat_v2_set(cursor(
select bs.bs_key from
  rc_database db,  
  rc_backup_set bs
where db.name='WMAIND01' and bs.db_key = db.db_key))
from dual;

select * from rc_backup_corruption;
select * from rc_copy_corruption;
