begin
  dbms_network_acl_admin.append_host_ace
  (
    host => 'oracle-base.com', 
    lower_port => 80,
    upper_port => 80,
    ace => xs$ace_type
    (
      privilege_list => xs$name_list('http'),
      principal_name => 'OK',
      principal_type => xs_acl.ptype_db
    )
  ); 
end;
/

begin
  dbms_network_acl_admin.append_host_ace
  (
    host => 'oracle-base.com', 
    lower_port => 80,
    upper_port => 80,
    ace => xs$ace_type
    (
      privilege_list => xs$name_list('connect'),
      principal_name => 'APEX_040200',
      principal_type => xs_acl.ptype_db
    )
  ); 
end;
/

SELECT * FROM dba_network_acls;
SELECT * FROM dba_host_acls;

SELECT * FROM dba_network_acl_privileges;
SELECT * FROM dba_host_aces;

begin
  dbms_network_acl_admin.remove_host_ace
  (
    'oracle-base.com',80,80,
    xs$ace_type(privilege_list => xs$name_list('http'), principal_name => 'OK', principal_type => xs_acl.ptype_db),
    true
  );
  
  commit;
end;
/

begin
  dbms_network_acl_admin.remove_host_ace
  (
    host => 'oracle-base.com', 
    lower_port => 80,
    upper_port => 80,
    ace => xs$ace_type(privilege_list => xs$name_list('CONNECT'), principal_name => 'APEX_040200', principal_type => xs_acl.ptype_db),
    remove_empty_acl => true
  );
end;
/
