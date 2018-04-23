with v_backups as
(
  select s.set_count, s.backup_type, p.device_type, p.piece#, p.tag, p.status, p.deleted 
  from
    v$backup_piece p,
    v$backup_set s
  where s.set_count = p.set_count
)
select * from v_backups
where device_type='DISK' and backup_type='L' and status='A';
