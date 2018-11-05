CREATE OR REPLACE FUNCTION split_string(p_string IN VARCHAR2, p_separator IN VARCHAR2 DEFAULT ',') RETURN tab_v256 IS
  ret    tab_v256;
BEGIN
  EXECUTE IMMEDIATE 
  'BEGIN :x := tab_v256('''||REPLACE(p_string, p_separator, ''',''')||'''); END;'
  USING OUT ret;

  RETURN ret;
END;
/

GRANT EXECUTE ON split_string TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM split_string FOR split_string;
