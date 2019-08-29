CREATE OR REPLACE FUNCTION split_string(p_string IN VARCHAR2, p_separator IN VARCHAR2 DEFAULT ',') RETURN tab_v256 IS
/*
  History of changes:
  ------------------------------------------------------------------------------
  28-Aug-2019, OK: added logic for processing NULL p_string value
*/
  ret    tab_v256;
BEGIN
  IF p_string IS NULL THEN
    RETURN tab_v256();
  ELSE
    EXECUTE IMMEDIATE 
    'BEGIN :x := tab_v256('''||REPLACE(p_string, p_separator, ''',''')||'''); END;'
    USING OUT ret;
  END IF;
  
  RETURN ret;
END;
/

GRANT EXECUTE ON split_string TO PUBLIC;
