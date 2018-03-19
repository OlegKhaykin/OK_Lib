create tablespace hadoop;

begin
  for r in
  (
    select 'CLOUDERA_'||t.column_value usr
    from table(split_string('MANAGER,OOZIE,SQOOP,AMON,RMAN,HIVE,HUE,SENTRY,NAV,NAVMS')) t
    left join dba_users u on u.username = 'CLOUDERA_'||t.column_value
    where u.username is null
  )
  loop
    execute immediate 'create user '||r.usr||' identified by m default tablespace hadoop temporary tablespace temp quota unlimited on hadoop';
  end loop;

  for r in
  (
    select username usr
    from dba_users
    where username like 'CLOUDERA%'
  )
  loop
    execute immediate 'grant CONNECT, SELECT ANY DICTIONARY, 
    CREATE ANY TABLE, ALTER ANY TABLE, DROP ANY TABLE, 
    CREATE ANY SEQUENCE, DROP ANY SEQUENCE, 
    CREATE ANY INDEX, ALTER ANY INDEX, DROP ANY INDEX,
    CREATE PROCEDURE, CREATE TRIGGER,
    CREATE DATABASE LINK, ALTER DATABASE LINK
    to '||r.usr;
  end loop;
end;
/

GRANT EXECUTE ON sys.dbms_crypto TO cloudera_nav;
GRANT CREATE VIEW TO cloudera_nav;

-- sudo /usr/share/cmf/schema/scm_prepare_database.sh oracle -h PowerEdge pdb1.oitc.com cloudera_manager m