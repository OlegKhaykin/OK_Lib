alter session set current_schema = debuger;

begin
  for r in
  (
    select * 
    from dba_tab_partitions
    where table_owner = 'DEBUGER'
    and table_name like 'DBG%'
    --and partition_name < 'SYS_P27378'
    and partition_name <> 'P1'
    order by partition_name desc
  )
  loop
    execute immediate 'alter table debuger.'||r.table_name||' drop partition '||r.partition_name;
  end loop;
end;
/

begin
  for r in
  (
    select i.index_name, ip.partition_name, ip.STATUS 
      --'alter index debuger.'||i.index_name||' rebuild partition '||ip.partition_name cmd
    from dba_indexes i
    left join dba_ind_partitions ip
      on ip.index_owner = i.owner
     and ip.index_name = i.index_name
    where i.owner = 'DEBUGER'
    and i.table_name like 'DBG%'
    and i.index_type <> 'LOB'
  )
  loop
    execute immediate r.cmd;
  end loop;
end;
/

ALTER TABLE DEBUGER.DBG_LOG_DATA ENABLE CONSTRAINT FK_LOGDATA_PROC;
ALTER TABLE DEBUGER.DBG_PERFORMANCE_DATA ENABLE CONSTRAINT FK_PERFDATA_proc;
ALTER TABLE DEBUGER.DBG_SUPPLEMENTAL_DATA ENABLE CONSTRAINT DBG_SUPPLDAT_FK_PROC;
