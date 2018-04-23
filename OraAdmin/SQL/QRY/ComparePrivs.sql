select * from 
(
  select
    u.*, 
    DECODE(d1.grantee, NULL, 'N', 'Y') wmaind01, 
    DECODE(d2.grantee, NULL, 'N', 'Y') wmaind02 
  from
    (
      select grantee, owner, table_name, privilege from dba_tab_privs
      union 
      select grantee, owner, table_name, privilege from dba_tab_privs@wmaind02
    ) u,
    dba_tab_privs d1,
    dba_tab_privs@wmaind02 d2
  where d1.grantee(+) = u.grantee and d1.owner(+) = u.owner and d1.table_name(+) = u.table_name and d1.privilege(+) = u.privilege
  and d2.grantee(+) = u.grantee and d2.owner(+) = u.owner and d2.table_name(+) = u.table_name and d2.privilege(+) = u.privilege
)
where wmaind01 = 'N' or wmaind02 = 'N' 

select * from 
(
  select
    u.*, 
    DECODE(d1.grantee, NULL, 'N', 'Y') wmaind01, 
    DECODE(d2.grantee, NULL, 'N', 'Y') wmaind02 
  from
    (
      select grantee, granted_role from dba_role_privs
      union 
      select grantee, granted_role from dba_role_privs@wmaind02
    ) u,
    dba_role_privs d1,
    dba_role_privs@wmaind02 d2
  where d1.grantee(+) = u.grantee and d1.granted_role(+) = u.granted_role
  and d2.grantee(+) = u.grantee and d2.granted_role(+) = u.granted_role
)
where wmaind01 = 'N' or wmaind02 = 'N' 

select * from 
(
  select
    u.*, 
    DECODE(d1.grantee, NULL, 'N', 'Y') wmaind01, 
    DECODE(d2.grantee, NULL, 'N', 'Y') wmaind02 
  from
    (
      select grantee, privilege from dba_sys_privs
      union 
      select grantee, privilege from dba_sys_privs@wmaind02
    ) u,
    dba_sys_privs d1,
    dba_sys_privs@wmaind02 d2
  where d1.grantee(+) = u.grantee and d1.privilege(+) = u.privilege
  and d2.grantee(+) = u.grantee and d2.privilege(+) = u.privilege
)
where wmaind01 = 'N' or wmaind02 = 'N' 
 
