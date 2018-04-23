set pages 10000
set echo on
spool badfiles.log

REM Tablespace name/s
select * from ts$ 
where ts# in ( select ts# from file$ where blocks>4194303 );

REM All files in those tablespaces
select * from file$ 
where ts# in ( select ts# from file$ where blocks>4194303 )
order by ts#,relfile#,blocks;

REM Bad files
select * from file$ where blocks > 4194303 order by ts#, relfile# ;
select * from v$datafile where blocks > 4194303 order by ts#, rfile# ;

REM Check for all segments in this tablespace/s
REM Note that bad file numbers can occur where the block number
REM has overflowed.
select s.* from seg$ s 
where ts# in ( select ts# from file$ where blocks>4194303 )
order by TS#,file#,block#;

REM Very Bad SEG$ entries
select s.* from seg$ s, file$ f
where s.file#=f.relfile#(+)
and s.ts#=f.ts#(+)
and f.file# is null;

REM Bad UET entries
select * from uet$ where block#+length-1 > 4194303
order by ts#,file#,block#;

REM Bad FET entries
select * from fet$ where block#+length-1 > 4194303
order by ts#,file#,block#;
