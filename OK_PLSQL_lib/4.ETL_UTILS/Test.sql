create sequence seq_tst_ok;

-- Target:
create table tst_ok
(
  id             INTEGER CONSTRAINT pk_tst_ok PRIMARY KEY,
  owner          VARCHAR2(128 BYTE) NOT NULL,
  object_name    VARCHAR2(128 BYTE) NOT NULL,
  object_type    VARCHAR2(30) NOT NULL,
  last_ddl_time  DATE NOT NULL,
  deleted_flag   CHAR(1) DEFAULT 'N' NOT NULL CHECK (deleted_flag IN ('N','Y')),
  CONSTRAINT uk_tst_ok UNIQUE(OWNER, object_name)
);

-- Source:
CREATE OR REPLACE VIEW v_ok_tables AS 
SELECT * FROM all_objects
WHERE owner = 'POC' AND object_type = 'TABLE';

-- Difference:
CREATE OR REPLACE VIEW v_table_diff AS
SELECT
  NVL(s.owner, t.owner) AS owner,
  NVL(s.object_name, t.object_name) AS object_name,
  t.object_type AS old_object_type,
  s.object_type,
  t.last_ddl_time AS old_ddl_time,
  s.last_ddl_time AS last_ddl_time, 
  t.ROWID AS row_id,
  CASE WHEN s.owner IS NOT NULL THEN 1 END AS etl$src_indicator,
  CASE WHEN s.owner IS NULL THEN 'Y' ELSE 'N' END AS deleted_flag
FROM
(
  SELECT * FROM all_objects s
  WHERE object_type = 'TABLE'
  AND owner = 'POC'
) s
FULL JOIN tst_ok t
  ON t.owner = s.owner AND t.object_name = s.object_name
WHERE s.owner IS NULL AND t.deleted_flag = 'N'
OR s.owner IS NOT NULL AND
(
  t.owner IS NULL 
  OR t.deleted_flag = 'Y'
  OR t.object_type <> s.object_type
  OR t.last_ddl_time <> s.last_ddl_time
);

alter session set nls_date_format = 'dd-Mon-yy hh24:mi:ss';
--------------------------------------------------------------------------------
-- Test #1:
begin
  xl.open_log('TST-OK', 'Initial population: INSERT with WHERE');

  pkg_etl_utils.add_data
  (
    p_operation       => 'replace',
    p_target          => 'tst_ok',
    p_source          => 'all_objects',
    p_where           => q'[owner = 'POC' AND object_type = 'TABLE']',
    p_hint            => 'all_rows',
    p_generate        => q'[id = seq_tst_ok.nextval]'
    , p_commit_at       => -1
--    , p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select count(1) from tst_ok; 

--------------------------------------------------------------------------------
-- Test #2, using MERGE:
begin
  xl.open_log('TST-OK', 'Merge from source veiw, no WHERE');

  pkg_etl_utils.add_data
  (
    p_operation       => 'MERGE', 
    p_target          => 'tst_ok',
    p_source          => 'v_ok_tables',    
    p_match_cols      => 'owner, object_name',
    p_generate        => q'[id, last_ddl_time=seq_tst_ok.nextval, sysdate]'
    , p_check_changed   => 'NONE'
    , p_commit_at       => -1
--    , p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_table_diff; -- should be no rows
select * from tst_ok;
update tst_ok set object_type = 'UNKNOWN';
commit;

--------------------------------------------------------------------------------
-- Test #3, logical deletion:
drop table tst_ok_dummy purge;
select * from v_table_diff where owner = 'OK'; -- should be 1 row

begin
  xl.open_log('TST-OK', 'Merge from source with WHERE condition and HINT, doing logical deletion, no versions');

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge', 
    p_target          => 'tst_ok',
    p_source          => 'all_objects',
    p_where           => q'[owner = 'POC' AND object_type = 'TABLE']',
    p_match_cols      => 'owner, object_name',
    p_hint            => 'all_rows',
    p_check_changed  => 'except object_type',
    p_delete          => 'if notfound then deleted_flag=''Y'':''N''',
    p_generate        => q'[id = seq_tst_ok.nextval]'
    , p_commit_at       => -1
--    , p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/
select * from v_table_diff; -- should be no rows
select * from tst_ok;

-- Test #4: physical deletion
begin
  xl.open_log('TST-OK', 'Simple merge from source with WHERE condition and HINT and physical deletion, no versions', 10);

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge', 
    p_target          => 'tst_ok',
    p_source          => 'all_objects',
    p_where           => q'[owner = 'POC' AND object_type = 'TABLE']',
    p_match_cols      => 'owner, object_name',
    p_hint            => 'all_rows',
    p_delete          => 'if notfound',
    p_generate        => q'[id = seq_tst_ok.nextval]'
--    , p_check_changed  => 'except object_type'
--    , p_commit_at       => -1
--    , p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_table_diff; -- should be no rows
select * from tst_ok;
rollback;
commit;
update tst_ok set last_ddl_time = sysdate;


-- Test #5: using ROWID for matching
create table tst_ok_dummy (n number);
select * from v_table_diff where owner = 'OK'; -- should be 1 row

begin
  xl.open_log('TST-OK', 'Merge by ROWID, no versions', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge', 
    p_target          => 'tst_ok',
    p_source          => 'v_table_diff',
    p_where           => q'[owner = 'OK']',
    p_match_cols      => 'rowid',
--    p_delete          => 'if notfound',
--    p_delete          => 'if s.deleted_flag=''Y''',
--    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
--    p_check_changed   => 'except object_type',                        -- should be ERROR!
    p_generate        => q'[id, source_system = seq_tst_ok.nextval, 'PCS']',
--    p_commit_at       => -1
    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_table_diff where owner = 'OK'; -- should be no rows
select * from tst_ok where object_name = 'TST_OK_DUMMY'; -- should be 1 row

-- ============================ WITH VERSIONING=================================

drop table tst_ok_dummy purge;
drop table tst_ok_ver purge;

CREATE TABLE tst_ok_ver
(
  object_vid      INTEGER PRIMARY KEY,
  owner           VARCHAR2(128 BYTE) NOT NULL,
  object_name     VARCHAR2(128 BYTE) NOT NULL,
  object_type     VARCHAR2(23 BYTE)  NOT NULL,
  last_ddl_time   DATE NOT NULL,
  source_system   VARCHAR2(30) NOT NULL,
  deleted_flag    CHAR(1) DEFAULT 'N' NOT NULL CHECK (deleted_flag IN ('N','Y')),
  version_num     NUMBER(10) NOT NULL,
  valid_from_dt   DATE NOT NULL,
  valid_until_dt  DATE NOT NULL,
  CONSTRAINT uk_tst_ok_ver UNIQUE (owner, object_name, version_num)
);

CREATE OR REPLACE VIEW v_tables_diff_ver AS
SELECT
  NVL(s.owner, t.owner) AS owner,
  NVL(s.object_name, t.object_name) AS object_name,
  NVL(s.object_type, t.object_type) AS object_type,
  NVL(s.last_ddl_time, t.last_ddl_time) AS last_ddl_time,
  t.last_ddl_time    AS old_ddl_time,
  s.last_ddl_time    AS new_ddl_time, 
  t.ROWID AS row_id,
  CASE WHEN s.owner IS NOT NULL THEN 1 END AS etl$src_indicator,
  NVL2(s.owner, 'N', 'Y') deleted_flag,
  NVL(t.version_num, 0) AS version_num 
FROM sys.all_objects s
FULL JOIN tst_ok_ver t
  ON t.owner = s.owner AND t.object_name = s.object_name
WHERE
(
  t.owner IS NULL OR
  t.valid_until_dt = DATE '9999-12-31'
  AND
  (
    t.object_type <> s.object_type OR
    t.last_ddl_time <> s.last_ddl_time OR
    t.deleted_flag = 'N' AND s.owner IS NULL
  )
)
AND NVL(s.object_type, t.object_type) = 'TABLE';

-- Test #1: 
select * from v_tables_diff_ver where owner = 'OK'; -- 17 rows

begin
  xl.open_log('TST-OK', 'Initial population with versions', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'replace',
    p_target          => 'tst_ok_ver',
    p_source          => 'all_objects',
    p_where           => q'[s.owner = 'OK' AND s.object_type = 'TABLE']',
    p_generate        => q'[source_system, object_vid = 'PCS', seq_tst_ok.nextval]',
--    p_check_changed  => 'except object_type',                         -- should be ERROR!
--    p_delete          => 'if notfound',                               -- should be ERROR!
--    p_delete          => q'[if notfound then deleted_flag='Y':'N']',  -- should be ERROR!
    p_versions        => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval '1' second]',
--    p_commit_at       => -1
    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_tables_diff_ver where owner = 'OK'; -- shoudl be no rows
select * from tst_ok_ver; -- should be 17 rows, all with VERSION=1

-- Test #2: MERGE with versioning

create table tst_ok_dummy (n number);

select * from v_tables_diff_ver where owner = 'OK'; -- shoudl be 1 row
select * from tst_ok_ver; -- should be 17 rows

begin
  xl.open_log('TST-OK', 'Merge with versioning', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge',
    p_target          => 'tst_ok_ver',
    p_source          => 'all_objects',
    p_where           => q'[s.object_type = 'TABLE' AND s.owner = 'OK']',
    p_match_cols      => 'owner, object_name',
    p_generate        => q'[source_system, object_vid = 'PCS', seq_tst_ok.nextval]',
    p_check_changed   => 'except object_type',
--    p_delete          => 'if notfound',                           -- should be ERROR!
    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
    p_versions        => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval '1' second]',
    p_commit_at       => -1
--    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_tables_diff_ver where owner = 'OK'; -- should be no rows
select * from tst_ok_ver order by object_name, version_num; -- should be 18 rows

-- Test #3: MERGE with 
insert into tst_ok_dummy values(1);
commit;
truncate table tst_ok_dummy; -- new DDL_TIME
select * from v_tables_diff_ver where owner = 'OK'; -- should be 1 row

begin
  xl.open_log('TST-OK', 'Merge with versioning', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'update', 
    p_target          => 'tst_ok_ver',
    p_source          => 'v_tables_diff_ver',
    p_where           => q'[s.owner = 'OK']',
    p_match_cols      => 'rowid',
    p_generate        => q'[source_system, object_vid = 'PCS', seq_tst_ok.nextval]',
--    p_check_changed   => 'except object_type',                    -- should be ERROR!
--    p_delete          => 'if notfound',                           -- should be ERROR!
    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
    p_versions        => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval '1' second]',
    p_commit_at       => -1
--    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/
select * from v_tables_diff_ver where owner = 'OK'; -- should be no rows
select * from tst_ok_ver t order by object_name, version_num; -- should be 19 rows, 1 with VERSION=2

-- Test #4: MERGE with DELETE and versioning
drop table tst_ok_dummy purge;
select * from v_tables_diff_ver where owner = 'OK'; -- should be 1 row

begin
  xl.open_log('TST-OK', 'Merge with delete and versioning', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'update', 
    p_target          => 'tst_ok_ver',
    p_source          => 'v_tables_diff_ver',
    p_where           => q'[s.owner = 'OK']',
    p_match_cols      => 'rowid',
    p_generate        => q'[source_system, object_vid = 'PCS', seq_tst_ok.nextval]',
--    p_check_changed   => 'except object_type',                    -- should be ERROR!
--    p_delete          => 'if notfound',                           -- should be ERROR!
    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
    p_versions        => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval '1' second]',
    p_commit_at       => -1
--    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from v_tables_diff_ver where owner = 'OK'; -- should be no rows
select * from tst_ok_ver t order by object_name, version_num; -- should be 20 rows, 1 with VERSION_NUM=2 and 1 with VERSION_NUM=3
