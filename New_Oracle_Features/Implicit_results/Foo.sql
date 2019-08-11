create or replace procedure foo as
  c1 sys_refcursor;
  c2 sys_refcursor;
begin
  open c1 for select * from hr.employees;
  dbms_sql.return_result(c1); --return to client

  open c2 for select * from hr.departments;
  dbms_sql.return_result (c2); --return to client
end;
/
