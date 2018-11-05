create or replace context etl_context using pkg_etl_utils_new;

drop table tst_ok purge;

create table tst_ok
(
  ID             INTEGER CONSTRAINT pk_tst_ok PRIMARY KEY,
  OWNER          VARCHAR2(128 BYTE) NOT NULL,
  OBJECT_NAME    VARCHAR2(128 BYTE) NOT NULL,
  OBJECT_TYPE    VARCHAR2(30) NOT NULL,
  LAST_DDL_TIME  DATE NOT NULL,
  source_system  VARCHAR2(30) NOT NULL,
  cob_date       DATE NOT NULL,
  deleted_flag   CHAR(1) DEFAULT 'N' NOT NULL CHECK (deleted_flag IN ('N','Y')),
  CONSTRAINT uk_tst_ok UNIQUE(OWNER, OBJECT_NAME)
);

update tst_ok set last_ddl_time = systimestamp;
commit; 

truncate table tst_ok;

EXEC pkg_etl_utils_new.set_parameter('cob_date', date '2018-11-03');

select eval_date(sys_context('etl_context','cob_date')) from dual;

begin
  xl.open_log('TST-OK', 'Simple merge', true);

  pkg_etl_utils_new.add_data
  (
    p_operation => 'merge',
    p_tgt => 'tst_ok',
    p_src => q'[SELECT * FROM all_objects WHERE owner = 'OK' AND object_type = 'TABLE']',
    p_match_cols => 'owner, object_name',
    p_generate => q'[id = seq_tst_ok.nextval; source_system = ''PCS''; cob_date = sys_context(''etl_context'',''cob_date'')]',
--    p_delete => q'[IF NOTFOUND then deleted_flag=''Y'':''N'']',
    p_delete => 'IF NOTFOUND',
    p_commit_at => 10
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select object_type, deleted_flag, count(1) cnt from tst_ok 
group by object_type, deleted_flag;

SELECT
  ',' || LISTAGG(name, ',') WITHIN GROUP (ORDER BY ROWNUM) v_gen_cols,  
  ',' || LISTAGG(value, ',') WITHIN GROUP (ORDER BY ROWNUM) v_gen_vals
FROM TABLE(get_name_values('id=seq_tst_ok.nextval'));

drop table tst_ok_ver purge;

CREATE TABLE tst_ok_ver
(
  object_vid      INTEGER PRIMARY KEY,
  owner           VARCHAR2(128 BYTE) NOT NULL,
  object_name     VARCHAR2(128 BYTE) NOT NULL,
  object_type     VARCHAR2(23 BYTE)  NOT NULL,
  last_ddl_time   DATE NOT NULL,
  version_num     NUMBER(10) NOT NULL,
  valid_from_dt   DATE NOT NULL,
  valid_until_dt  DATE NOT NULL,
  CONSTRAINT uk_tst_ok_ver UNIQUE (owner, object_name, version_num)
);


truncate table tst_ok_ver;

begin
  xl.open_log('TST-OK', 'Merge with versioning', true);

  pkg_etl_utils_new.add_data
  (
    p_operation => 'REPLACE',
    p_tgt => 'tst_ok_ver',
    p_src => q'[SELECT * FROM all_objects WHERE owner = 'OK' AND object_type = 'TABLE']',
--    p_match_cols => 'owner, object_name',
    p_generate => 'object_vid=seq_tst_ok.nextval',
--    p_delete => 'IF NOTFOUND',
    p_versioning => q'[version_num; valid_from_dt=sysdate; valid_until_dt=sysdate-interval ''1'' second]',
    p_commit_at => -1
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/

select count(1) cnt from tst_ok_ver t;