begin
  dbms_stats.gather_database_stats(degree=>2,cascade=>TRUE);
end;
/
