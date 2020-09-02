alter session set current_schema = ahmadmin;

SELECT * FROM TABLE(ahmadmin.psa_regression_test.totals(7286, 7281));

select
  proc_id, name, comment_txt, result, start_time, end_time,
  case when days > 1 then days||' days ' when days > 0 then '1 day ' end ||
  case when days > 0 or hours > 0 then hours || ' hr ' end ||
  case when days > 0 or hours > 0 or minutes > 0 then minutes || ' min ' end ||
  round(seconds, 3)|| ' sec' time_spent
from
(
  select
    proc_id, name, comment_txt, result, start_time, end_time,
    extract(day from diff) days,
    extract(hour from diff) hours,
    extract(minute from diff) minutes,
    extract(second from diff) seconds
  from
  (
    select l.*, nvl(end_time, systimestamp) - start_time diff 
    from debuger.dbg_process_logs l
    where 1=1
    --and comment_txt like '%P_USER=Oleg%'
    --and proc_id in 7101
    --and start_time > sysdate - 30
    --and name like '%'
    --and comment_txt LIKE '%00000004176878110344%'
    --and result like '%Overlapping PSA_BPU_SUPPLIER_XREF%'
  )
)
order by proc_id desc;


--=========================== DETAILS ==========================================
with
  det as
  (
    select
      proc_id,
      --trunc(tstamp, 'HH') hr, 
      tstamp, pls_unit, 
      log_depth, action, to_char(substr(comment_txt,1,2000)) result
    from debuger.dbg_log_data
    where 1=1
    and proc_id IN (5160)
    --and pls_unit <> 'PSA_REGRESSION_TEST'
    --and pls_unit = 'ODS_ACCOUNTSUPPLIERSYNC'
    and 
    (
      --1=0 and   -- nothing
      --1=1 or  -- everything
      action in 
      (
        --'Preparing next placement',
        'ADD_DATA',
        'Executing DML command',
--        'Executing command',
        --'Processing Control','Processing [CSA+Plan]','ROLLBACK TO SAVEPOINT','SPT_UPDATESTATUS_STAGINGODS','SET_STG_CONTROL_STATUS',
        --'PROCESS_SEGM_CHANGE_REQ',
        --'SPT_CREATE_MOVE_SUPPLIER','SPT_DERIVE_BY_BPLV_PRODUCT','SPT_CREATE_CSA_SUPP_BPLV',
        --'SPT_CREATE_SUPPLIER','SPT_ADD_PRODUCT','INSERT INTO Supplier','UPDATE Supplier','INSERT INTO SupplierAcctPackageXREF','INSERT INTO PurchasedProductCommSetup',
        --'LOAD_TMP_EX_SUPP_CSA_PROD','INSERT_TMP_EX_SUPP_CSA_PROD',
        --'LOAD_TMP_EX_SUPP_CSA_PRDLIST','INSERT_TMP_EX_SUPP_CSA_PRDLIST',
        --'Decision point'
        --'SPT_CREATE_CSA','SPT_TERM_EXISTING_CSA','SPT_PROCESS_PURGE','PROCESS_PURGE_REQUESTS',
        --'INSERT INTO PlansponsorControlINFO','INSERT INTO ControlSuffixAccount',
        --'INSERT INTO SupplierCSAXREF','UPDATE SupplierCSAXREF',*/
        --'DELETE FROM CSABPLVXREF','INSERT INTO CSABPLVXREF','UPDATE CSABPLVXREF','MERGE INTO CSABPLVXREF'
        --'Deciding on the Task Type','MANAGE_SUPPLIER_TASK',
        --'FN_ISDUPLICATERESULT','IS_DUPLICATE_RESULT','ADD_RESULT','Adding result into TBL',
        --'SPT_ODS_SYNCUP', 'Processing Action', 'Processing Task',
        --
        --'SP_TRACK_CSASUPPLIERMOVE',
        --'SP_UPDATESUPPLIER','SP_CREATESUPPLIER',
        --'INSERT INTO InsuranceOrganization','UPDATE InsuranceOrganization','INSERT INTO MasterSupplierOrganization','UPDATE MasterSupplierOrganization',
        --'Checking if this Supplier Organization is already known to ODS',
        --'MERGE INTO SupplierPlansponsrRelationship','INSERT INTO SupplierPlanSponsrRelationship',*/
        'Dummy'
      ) -- 4/10/2020 2:53:16.023024 PM
      --or tstamp between to_timestamp('8/3/2020 7:33:43.716140 AM', 'mm/dd/yyyy hh:mi:ss.FF6 AM') and to_timestamp('8/3/2020 7:33:43.747355 AM','mm/dd/yyyy hh:mi:ss.FF6 AM')
      --or comment_txt like '%ORA-%'
      --and comment_txt like 'MASK%'
    )
    and pls_unit <> 'PSA_REGRESSION_TEST'
    --and log_depth < 3
  )
select --+ parallel(32) 
  * from det
--where (proc_id, pls_unit, log_depth, action) in (select proc_id, pls_unit, log_depth, action from det where result like 'ORA%') 
order by tstamp ;--desc;  -- This is the main method to see the log data
--select result, count(1) cnt from det where result <> 'Done processing CSA' group by result;
  , ord as -- numbering log rows: "begin" rows get RNUM=1, "end" rows get RNUM=2; 
  (
    select
      d.*, 
      row_number() over(partition by proc_id order by tstamp) row_num,
      mod(row_number() over(partition by proc_id, log_depth, action order by tstamp) -1, 2)+1 rnum -- 1 or 2
    from det d
  )
--select * from ord /*where rnum = 1*/ order by row_num, proc_id;  
  , tstat as -- time statistics: how long it took from begin to end of an action
  (
    select proc_id, tstamp, pls_unit, log_depth, action, result, extract(minute from dur)*60 + round(extract(second from dur), 3) sec
    from
    (
      select
        o.*, 
        case when rnum = 1 then lead(tstamp) over(partition by log_depth, pls_unit, action order by tstamp) - tstamp end dur
      from ord o
    )
    where rnum = 1
  )
select * from tstat order by sec desc;  
select action, count(1) cnt, sum(sec) total_sec, round(avg(sec), 3) avg_sec from tstat group by action order by total_sec desc;

--========================== PERFORMANCE =======================================
select
  proc_id, action, cnt, round(seconds, 3) sec,
  round(seconds/cnt, 6) sec_per_run
from debuger.dbg_performance_data 
where proc_id in (6339)
order by sec desc, proc_id;


--========================== SUPPLEMENTAL ======================================
select
  dsd.proc_id, dsd.name, dsd.tstamp, dsd.value.GetTypeName(),
  t.*
from dbg_supplemental_data dsd
cross join table(get_tab_test_val(dsd.value)) t;

--========================== SETTINGS ======================================
merge into debuger.dbg_settings tgt
using
(
  select 'PSA_DATA_INGEST.SPM_GET_SUPPLIER_DETAILS' proc_name, 5 log_level from dual
  union all select 'PKG_PRODUCT_SETUP_AUTOMATION.ARCHIVE_PERS_DATA', 5 from dual 
  union all select 'PKG_PRODUCT_SETUP_AUTOMATION.PROCESS_PERS_DATA', 5 from dual 
  union all select 'PSA_DATA_INGEST.REGISTER_SEGM_CHANGE_REQ' proc_name, 5 log_level from dual
  union all select 'PSA_DATA_INGEST.SPT_PROCESS_SUPPLIER', 5 from dual
  union all select 'PSA_DATA_INGEST.SPM_GET_SUPPLIER_PAYLOAD_AA', 5 from dual
  union all select 'PSA_DATA_INGEST.SPM_GET_SUPPLIERBUNDLEDETAILS', 5 from dual
) src
on (tgt.proc_name = src.proc_name)
when matched then update set tgt.log_level = src.log_level where tgt.log_level <> src.log_level
when not matched then insert values(src.proc_name, src.log_level);  

commit;

select * from debuger.dbg_settings;

select xl.get_current_proc_id from dual;

update debuger.dbg_process_logs set result = 'Cancelled', end_time = systimestamp where proc_id in (183) and end_time is null;
commit;

select distinct ErrorProcessIND from ods.supplierbatch; 
