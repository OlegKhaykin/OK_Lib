select * from dba_users where username = user;

select * from dba_profiles where profile = --'USER_PROFILE';
(select profile from dba_users where username = user)
;

select * from dba_rsrc_consumer_groups;

select * from user_sys_privs;