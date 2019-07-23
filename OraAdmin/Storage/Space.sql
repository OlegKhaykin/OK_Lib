select
  d.tablespace_name,
  d.total_mb,
  s.used_mb,
  f.available_mb,
  ROUND(f.available_mb/d.total_mb*100, 1) pct_free
from
  (
    select
      tablespace_name,
      ROUND(sum(bytes)/1024/1024,1) total_mb
    from dba_data_files
    WHERE 1=1
--    and tablespace_name NOT IN (SELECT tablespace_name FROM dba_data_files WHERE autoextensible = 'YES')
--    and tablespace_name like 'UNDO%'
    group by tablespace_name
  ) d,
  (
    select
      tablespace_name,
      Round(sum(bytes)/1024/1024,1) used_mb
    from dba_segments
    GROUP BY tablespace_name
  ) s,
  (
    select
      tablespace_name,
      NVL(Round(sum(bytes)/1024/1024,1),0) available_mb
    from dba_free_space
    group by tablespace_name
  ) f
where s.tablespace_name(+) = d.tablespace_name
and f.tablespace_name(+) = d.tablespace_name
--and nvl(f.available_mb,0)/d.total_mb < 0.05
order by tablespace_name
;

select
  tablespace_name, segment_name, segment_type, round(bytes/1024/1024) mbytes
--  round(sum(bytes)/1024/1024) mbytes
from dba_segments
where 1=1
and segment_name like 'TST_OK%'
--and tablespace_name = 'FUSION_DATA_RAW'
order by mbytes,
tablespace_name, segment_name
--order by mbytes desc
;

select tablespace_name, segment_name, segment_type, round(sum(bytes)/1024/1024) mbytes
from dba_segments
where owner = 'ODS'
group by tablespace_name, segment_name, segment_type
order by mbytes desc
;

-- TEMP space:
SELECT * FROM dba_temp_files;
