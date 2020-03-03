BEGIN
  dbms_network_acl_admin.append_host_ace
  (
    host => '*',
    ace => sys.xs$ace_type
    (
      privilege_list => sys.xs$name_List('JDWP') ,
      principal_name => 'N384433',
      principal_type => sys.xs_acl.ptype_db
    )
  );
END;
/

SELECT * FROM dba_host_acls;
SELECT * FROM dba_host_aces;

begin
  dbms_network_acl_admin.remove_host_ace
  (
    host => 'toshiba.home',
    ace => sys.xs$ace_type
    (
      privilege_list => sys.xs$name_list('JDWP') ,
      principal_name => 'OK',
      principal_type => sys.xs_acl.ptype_db
    ),
    remove_empty_acl => true
  );
end;
/

