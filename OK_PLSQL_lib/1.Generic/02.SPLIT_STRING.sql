CREATE OR REPLACE FUNCTION split_string(p_string IN VARCHAR2, p_separator IN VARCHAR2 DEFAULT ',') RETURN tab_v256 IS
/*
  History of changes:
  ------------------------------------------------------------------------------
  29-Jan-2021, OK: new version.
*/
  n_len       PLS_INTEGER;
  n_sep_len   PLS_INTEGER;
  p1          PLS_INTEGER;
  p2          PLS_INTEGER;
  i           PLS_INTEGER := 0;
  
  ret         tab_v256;
BEGIN
  ret := tab_v256();
  
  IF p_string IS NOT NULL THEN
    n_len := LENGTH(p_string);
    n_sep_len := LENGTH(p_separator);
    p1 := 1;
    
    LOOP
      ret.EXTEND;
      i := i+1;
      p2 := INSTR(p_string, p_separator, p1);
      ret(i) := SUBSTR(p_string, p1, CASE p2 WHEN 0 THEN n_len ELSE p2-p1 END);
      EXIT WHEN p2 = 0;
      p1 := p2 + n_sep_len;
    END LOOP;
  END IF;
  
  RETURN ret;
END;
/

GRANT EXECUTE ON split_string TO PUBLIC;