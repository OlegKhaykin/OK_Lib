select terminal, program, username, sid, serial# from v$session where status='ACTIVE' order by username;
