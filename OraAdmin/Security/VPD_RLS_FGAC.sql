/* From Database Security Guide

  DBMS_RLS Procedures:
  
  For Handling Individual Policies
  ------------------------------------- 
  DBMS_RLS.ADD_POLICY               -- Adds a policy to a table, view, or synonym;
  DBMS_RLS.ENABLE_POLICY            -- Enables (or disables) a policy you previously added to a table, view, or synonym
  DBMS_RLS.ALTER_POLICY             -- Alters an existing policy to associate or disassociate attributes with the policy
  DBMS_RLS.REFRESH_POLICY           -- Invalidates cursors associated with nonstatic policies
  DBMS_RLS.DROP_POLICY              -- To drop a policy from a table, view, or synonym
  
  For Handling Grouped Policies:
  -------------------------------------
  DBMS_RLS.CREATE_POLICY_GROUP      -- Creates a policy group
  DBMS_RLS.ALTER_GROUPED_POLICY     -- Alters a policy group 
  DBMS_RLS.DELETE_POLICY_GROUP      -- Drops a policy group
  DBMS_RLS.ADD_GROUPED_POLICY       -- Adds a policy to the specified policy group
  DBMS_RLS.ENABLE_GROUPED_POLICY    -- Enables a policy within a group
  DBMS_RLS.REFRESH_GROUPED_POLICY   -- Parses again the SQL statements associated with a refreshed policy
  DBMS_RLS.DISABLE_GROUPED_POLICY   -- Disables a policy within a group
  DBMS_RLS.DROP_GROUPED_POLICY
  
  For Handling Application Contexts:
  -------------------------------------
  DBMS_RLS.ADD_POLICY_CONTEXT       -- Drops a policy that is a member of the specified group
  DBMS_RLS.DROP_POLICY_CONTEXT      -- Adds the context for the active application
*/

select * from dba_policies
where object_owner = 'AHM_ADMIN'
and object_name like 'CASE_RECOMMEND%';

select * from dba_policy_attributes;
select * from dba_policy_contexts;
select * from dba_policy_groups;
select * from dba_sec_relevant_cols;

begin
  dbms_rls.add_policy
  (
    object_schema => 'ahm_admin',
    object_name => 'case_recommend',
    policy_name => 'case_rec_pol_ok',
    function_schema => 'ahm_admin',
    policy_function => 'custom_security_policies.hmc_case_recommend',
    statement_types => 'select,insert,update,delete',
    update_check => true, -- in 12c, has to be TRUE if we want to apply this policy to INSERT, it was not necessary in 11g 
    enable => true,
    static_policy => false
  );
end;
/

exec dbms_rls.drop_policy('ahm_admin','case_recommend_old','case_rec_pol');

--AHM_ADMIN	CASE_RECOMMEND_OLD	SYS_DEFAULT	CASE_REC_POL	AHM_ADMIN	CUSTOM_SECURITY_POLICIES	HMC_CASE_RECOMMEND	YES	YES	YES	YES	NO	NO	YES	NO	DYNAMIC	NO	NO	NO
