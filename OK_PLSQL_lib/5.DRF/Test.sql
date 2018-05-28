insert into cnf_data_flows values('TST_OBJECTS', 'Test data flow', 10, SYSDATE, NULL, 'STOP');

truncate table cnf_data_flow_steps;
insert into cnf_data_flow_steps values('TST_OBJECTS', 1, 1, 'MERGE', 'TST_OK', 'ALL_OBJECTS', NULL, 'OWNER,OBJECT_NAME', 'WHERE object_type IN (''TABLE'',''VIEW'')', NULL, -1, 'Y');

commit;

select * from cnf_data_flows;
select * from cnf_data_flow_steps;

exec drf.start_data_flow('TST_OBJECTS');
