select * from all_objects where object_name like '%FLASH%'
and object_type = 'VIEW';

select * from dba_flashback_archive;
select * from dba_flashback_archive_ts;
select * from dba_flashback_archive_tables;
select * from dba_segments where segment_name = 'SYS_FBA_HIST_131701';

select segment_name, round(sum(bytes)/1024/1024) mbytes
from dba_segments
where owner = 'FUSION'
group by segment_name;