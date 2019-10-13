/*
# Relevant Initialization parameters:
# Process global area (PGA):
# - WORKAREA_SIZE_POLICY: AUTO or MANUAL
# - PGA_AGGREGATE_TARGET: total amount of private memory for all processes
# - SORT_AREA_SIZE: obsolete parameter
#
# System Global Area (SGA)
#-----------------------------------------------------------------------------------------------
# - MAX_SGA_SIZE: absolute maximum for SGA if manually configured pools do not exceed this value
#                 if they do, this parameter is ignored;
*/
-- Current SGA component sizes:
SELECT component, current_size, Round(current_size/1024/1024) mb
FROM v$sga_dynamic_components;

-- Predicted values of physical reads for different sizes of the buffer cache
SELECT
  size_for_estimate,
  buffers_for_estimate,
  estd_physical_read_factor physcal_reads_increase,
  estd_physical_reads estimated_reads
FROM v$db_cache_advice
WHERE name = 'DEFAULT'
AND block_size = (SELECT value FROM v$parameter WHERE name = 'db_block_size')
AND advice_status = 'ON';

-- Current statistics about buffer hit ratio
SELECT s.*,
  ROUND((consistent_gets+db_block_gets-physical_reads)/(consistent_gets+db_block_gets),3) hit_ratio
FROM
( 
  SELECT
    cg.value consistent_gets,
    dbg.value db_block_gets,
    pr.value physical_reads
  FROM
    v$sysstat cg,
    v$sysstat dbg,
    v$sysstat pr
  WHERE cg.name = 'consistent gets'
  AND dbg.name = 'db block gets'
  AND pr.name = 'physical reads'
) s;
