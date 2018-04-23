begin
/*  for r in
  (
    select username from dba_users
    where username like 'JCREW_PUBLISH%'
  )
  loop*/
--    dbms_stats.unlock_schema_stats(r.username);
    dbms_stats.gather_schema_stats(ownname=>'UAT_JCREW_DSS',estimate_percent=>20,cascade=>true);
--  end loop;
end;


select owner, max(last_analyzed) from dba_tables
/*
where owner in 
(
  select username from dba_users
  where username like 'JCREW%'
)*/
GROUP by owner
having max(last_analyzed) < sysdate-1;

select owner, table_name, stattype_locked from dba_tab_statistics where owner='JC_CUSTOMERDB' order by owner;


exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_order_lines', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_order_headers', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_order_events', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_products', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_shipment_header', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_shipment_line', cascade=>true);
exec dbms_stats.gather_table_stats(ownname=>'uat_jcrew_dss', tabname=>'mine_customers', cascade=>true);

exec dbms_stats.gather_table_stats(ownname=>'dof', tabname=>'det_order_line_events', cascade=>true);
