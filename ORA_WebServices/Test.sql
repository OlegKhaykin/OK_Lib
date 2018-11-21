select * from all_objects where object_name in ('XS$ACE_TYPE','XS$NAME_LIST');

select * from table(xs$name_list('http','tcp'));

SELECT username,
       account_status,
       TO_CHAR(lock_date, 'DD-MON-YYYY') AS lock_date,
       TO_CHAR(expiry_date, 'DD-MON-YYYY') AS expiry_date,
       default_tablespace,
       temporary_tablespace
FROM   dba_users
WHERE  username LIKE UPPER('%APEX%')
ORDER BY username;
