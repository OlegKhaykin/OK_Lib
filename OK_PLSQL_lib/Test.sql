drop table tst_ok purge;

create table tst_ok
(
  ID             INTEGER CONSTRAINT pk_tst_ok PRIMARY KEY,
  OWNER          VARCHAR2(128 BYTE) NOT NULL,
  OBJECT_NAME    VARCHAR2(128 BYTE) NOT NULL,
  OBJECT_TYPE    VARCHAR2(30) NOT NULL,
  LAST_DDL_TIME  DATE NOT NULL,
  source_system  VARCHAR2(30) NOT NULL,
  deleted_flag   CHAR(1) DEFAULT 'N' NOT NULL CHECK (deleted_flag IN ('N','Y')),
  CONSTRAINT uk_tst_ok UNIQUE(OWNER, OBJECT_NAME)
);

CREATE OR REPLACE VIEW v_all_tables AS
SELECT
  NVL(s.owner, t.owner) AS owner,
  NVL(s.object_name, t.object_name) AS object_name,
  NVL(s.object_type, t.object_type) AS object_type,
  NVL(s.last_ddl_time, t.last_ddl_time) AS last_ddl_time,
  t.ROWID AS row_id,
  CASE WHEN s.owner IS NOT NULL THEN 1 END AS etl$src_indicator,
  CASE WHEN s.owner IS NULL THEN 'Y' ELSE 'N' END AS deleted_flag
FROM sys.all_objects s
FULL JOIN tst_ok t
  ON t.owner = s.owner AND t.object_name = s.object_name
WHERE s.owner IS NULL AND t.deleted_flag = 'N'
OR s.object_type = 'TABLE' AND
(
  t.owner IS NULL 
  OR t.object_type <> s.object_type
  OR t.last_ddl_time <> s.last_ddl_time
);

select * from v_all_tables;

truncate table tst_ok;

begin
  xl.open_log('TST-OK', 'Initial population, no versions', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'replace',
    p_target          => 'tst_ok',
    p_source          => 'all_objects',
    p_where           => q'[owner = 'OK' AND object_type = 'TABLE']',
    p_hint            => 'all_rows',
    p_generate        => q'[id, source_system = seq_tst_ok.nextval, 'PCS']',
    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

begin
  xl.open_log('TST-OK', 'Simple merge, no versions', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge', 
    p_match_cols      => 'owner, object_name',
    p_target          => 'tst_ok',
    p_source          => 'all_objects',
    p_where           => q'[owner = 'OK' AND object_type = 'TABLE']',
    p_hint            => 'all_rows',
    p_check_changed  => 'except object_type',
--    p_delete          => 'if notfound',
    p_delete          => q'[IF NOTFOUND then deleted_flag='Y':'N']',
    p_generate        => q'[id, source_system = seq_tst_ok.nextval, 'PCS']',
    p_commit_at       => -1
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

begin
  xl.open_log('TST-OK', 'Merge by ROWID, no versions', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'merge', 
    p_target          => 'tst_ok',
    p_source          => 'v_all_tables',
    p_where           => q'[owner = 'OK']',
    p_hint            => 'all_rows',
    p_match_cols      => 'rowid',
--    p_delete          => 'if notfound',
--    p_delete          => 'if s.deleted_flag=''Y''',
--    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
--    p_check_changed   => 'except object_type',                        -- should be ERROR!
    p_generate        => q'[id, source_system = seq_tst_ok.nextval, 'PCS']',
    p_commit_at       => -1
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from tst_ok order by object_name;
select * from v_all_tables where owner = 'OK';

-- ============================ WITH VERSIONING=================================

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

CREATE OR REPLACE VIEW v_all_tables_ver AS
SELECT
  NVL(s.owner, t.owner) AS owner,
  NVL(s.object_name, t.object_name) AS object_name,
  NVL(s.object_type, t.object_type) AS object_type,
  NVL(s.last_ddl_time, t.last_ddl_time) AS last_ddl_time,
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

select * from v_all_tables_ver where owner = 'OK';

truncate table tst_ok_ver;

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
    p_commit_at       => -1
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

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
    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

begin
  xl.open_log('TST-OK', 'Merge with versioning', true);

  pkg_etl_utils.add_data
  (
    p_operation       => 'update', 
    p_target          => 'tst_ok_ver',
    p_source          => 'v_all_tables_ver',
    p_where           => q'[s.owner = 'OK']',
    p_match_cols      => 'rowid',
    p_generate        => q'[source_system, object_vid = 'PCS', seq_tst_ok.nextval]',
--    p_check_changed   => 'except object_type',                    -- should be ERROR!
--    p_delete          => 'if notfound',                           -- should be ERROR!
--    p_delete          => q'[if notfound then deleted_flag='Y':'N']',
    p_versions        => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval '1' second]',
    p_commit_at       => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select * from tst_ok_ver t order by object_name, version_num;
select * from v_all_tables_ver where owner = 'OK' order by object_name, version_num;
