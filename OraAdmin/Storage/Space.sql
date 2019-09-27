alter session enable parallel dml;
alter session enable parallel ddl;

--CREATE TABLE tst_ok_space_usage PARALLEL 16 AS 
SELECT
  d.tablespace_name, d.max_mb, d.allocated_mb,
  NVL(s.used_mb, 0) used_mb, 
  NVL(f.available_mb, 0) available_mb,
  ROUND(NVL(f.available_mb, 0)/d.allocated_mb*100) allocated_free_pct, 
  ROUND((d.max_mb - NVL(s.used_mb, 0))*100/d.max_mb) max_free_pct
FROM
  (
    SELECT
      tablespace_name,
      ROUND(SUM(bytes)/1024/1024)  allocated_mb,
      ROUND(SUM(DECODE(maxbytes, 0, bytes, maxbytes))/1024/1024)  max_mb
    FROM dba_data_files
    WHERE 1=1
--    and tablespace_name NOT IN (SELECT tablespace_name FROM dba_data_files WHERE autoextensible = 'YES')
--    and tablespace_name like 'UNDO%'
    GROUP BY tablespace_name
  ) d,
  (
    SELECT
      tablespace_name,
      ROUND(SUM(bytes)/1024/1024) used_mb
    FROM dba_segments
    GROUP BY tablespace_name
  ) s,
  (
    SELECT
      tablespace_name,
      NVL(ROUND(SUM(bytes)/1024/1024),0) available_mb
    FROM dba_free_space
    GROUP BY tablespace_name
  ) f
WHERE s.tablespace_name(+) = d.tablespace_name
AND f.tablespace_name(+) = d.tablespace_name
--and nvl(f.available_mb,0)/d.total_mb < 0.05
;
