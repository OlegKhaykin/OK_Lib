--###### SYS #############################################################
begin
  dbms_repair.admin_tables(table_type=>dbms_repair.repair_table, action=>dbms_repair.create_action);
  dbms_repair.admin_tables(table_type=>dbms_repair.orphan_table, action=>dbms_repair.create_action);
end;
/

grant all on repair_table to okhaykin_dba;
grant all on orphan_key_table to okhaykin_dba;

--###########################################################
create or replace synonym dbms_repair for sys.dbms_repair;
create or replace synonym repair_table for sys.repair_table;
create or replace synonym orphan_key_table for sys.orphan_key_table;

create table my_extents
(
  file_id           number(10),
  block_id          number(10),
  blocks            number(10) not null,
  owner             varchar2(30) not null,
  segment_name      varchar2(30) not null,
  partition_name    varchar2(30),
  tablespace_name   varchar2(30) not null,
  segment_type      varchar2(30) not null,
  constraint pk_extents primary key(file_id, block_id)
);
create index ids_segment_type on my_extents(segment_type);

create or replace view vw_corrupt_segments as
select distinct owner, segment_type, segment_name
from 
  v$database_block_corruption c,
  my_extents e
where e.file_id = c.file#
and e.block_id <= c.block# + c.blocks - 1 and e.block_id + e.blocks - 1 >= c.block#
and e.segment_type in ('TABLE', 'TABLE PARTITION','INDEX','INDEX PARTITION','CLUSTER');

create or replace package pkg_admin as
  procedure get_tablespace_extents(p_tablespace_name in varchar2);
  procedure get_possibly_corrupt_extents;
  procedure get_corruption_details;
end;
/

create or replace package body pkg_admin as
  function get_segment_type_id(segment_type in varchar2) return pls_integer is
    ret pls_integer;
  begin
    ret := case
      when segment_type like 'TABLE%' then dbms_repair.table_object
      when segment_type like 'INDEX%' then dbms_repair.index_object
      when segment_type = 'CLUSTER' then dbms_repair.cluster_object
      else 0
    end;
    return ret;
  end;

  procedure get_tablespace_extents(p_tablespace_name in varchar2) is
    action VARCHAR2(128);
  begin
    action := 'Getting extents for tablespace '||p_tablespace_name;
    xl.write(action,'Started');
    for r in
    (
      select owner, segment_name, partition_name, file_id, block_id, blocks, tablespace_name, segment_type
      from dba_extents where tablespace_name = p_tablespace_name
    )
    loop
      insert into my_extents 
      values(r.file_id, r.block_id, r.blocks, r.owner, r.segment_name, r.partition_name, r.tablespace_name, r.segment_type);
    end loop;
    commit;
    xl.write(action,'Done');
  end;
  
  procedure get_possibly_corrupt_extents is
    action varchar2(128);
  begin
    action := 'Truncating table MY_EXTENTS';
    xl.write(action,'Started');
    execute immediate 'truncate table my_extents';
    xl.write(action,'Done');
    
    action := 'Getting extents from suspicious tablespaces';
    xl.write(action,'Started');
    for r in 
    (
      select distinct f.tablespace_name
      from 
        v$database_block_corruption c,
        dba_data_files f
      where f.file_id = c.file#
    )
    loop
      get_tablespace_extents(r.tablespace_name);
    end loop;
    xl.write(action,'Done');
  end;

  procedure get_corruption_details is
    cnt   pls_integer;
    act   varchar2(128);
  begin
    xl.open('Corruption check', true);
    
    act := 'Cleaning tables REPAIR_TABLE and ORPHAN_KEY_TABLE';
    xl.write(act, 'Started');
    delete from repair_table;
    delete from orphan_key_table;
    xl.write(act, 'Done');
    
    act := 'Finding segments that may be corrupt';
    xl.write(act, 'Started');
    get_possibly_corrupt_extents;
    xl.write(act, 'Done');
    
    for r in (select * from vw_corrupt_segments) loop
      act := 'Checking '||r.segment_type||' '||r.owner||'.'||r.segment_name;
      xl.write(act, 'Started');
      dbms_repair.check_object
      (
        schema_name=>r.owner, 
        object_type=>get_segment_type_id(r.segment_type), 
        object_name=>r.segment_name, 
        corrupt_count=>cnt
      );
      xl.write(act, cnt || ' corrupt blocks found');
      
      act := 'Dumping '||r.owner||'.'||r.segment_name||'entries that reference corrupt blocks';
      xl.write(act, 'Started');
      if r.segment_type like 'INDEX%' then
        dbms_repair.dump_orphan_keys(r.owner, r.segment_name, key_count=>cnt);
      end if;
      xl.write(act, cnt || ' keys dumped');
    end loop;
    
    xl.close('Successfully completed');
  exception
   when others then
    xl.close(sqlerrm);
  end;
  
  procedure fix_corrupt_blocks is
    cnt     pls_integer;
    act     varchar2(128);
  begin
    xl.open('Fixing corrupt blocks', true);
    for r in (select * from vw_corrupt_segments) loop
      act := 'Fixing '||r.segment_type||' '||r.owner||'.'||r.segment_name;
      xl.write(act, 'Started');
      dbms_repair.fix_corrupt_blocks
      (
        schema_name=>r.owner,
        object_type=>get_segment_type_id(r.segment_type),
        object_name=>r.segment_name, fix_count=>cnt
      );
      xl.write(act, 'Blocks fixed: '||cnt);
    end loop;
    xl.close('Successfully completed');
  exception
   when others then
    xl.close(sqlerrm);
  end;
end;
/

--#################################################################################
select * from v$database_block_corruption;
select * from v$copy_corruption;

begin
  pkg_admin.get_corruption_details;
END;
/

select * from vw_corrupt_segments;
select * from repair_table;
select * from orphan_key_table;

SELECT
  proc_id,
  description,
  To_Char(start_time, 'mm/dd/yy HH24:MI:SS.FF3') start_time,
  To_Char(end_time, 'mm/dd/yy HH24:MI:SS.FF3') end_time,
  result
FROM dbg_process_logs ORDER BY proc_id DESC;

SELECT
  proc_id, TO_CHAR(tstamp, 'HH24:MI:SS.FF3') time_stamp,
  action, comment_txt
FROM dbg_log_data
WHERE proc_id = (select max(proc_id) from dbg_process_logs where description in ('Corruption check'))
ORDER BY proc_id DESC, tstamp;

SELECT proc_id, action, cnt, Round(seconds, 2) seconds
FROM dbg_performance_data
WHERE proc_id=21
ORDER BY proc_id DESC, seconds DESC, cnt DESC;

SELECT * FROM dbg_exceptions WHERE proc_id=15;
