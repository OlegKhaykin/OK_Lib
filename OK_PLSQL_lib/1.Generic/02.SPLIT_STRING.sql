CREATE OR REPLACE FUNCTION split_string(p_string IN VARCHAR2) RETURN tab_v256 IS
  ret    tab_v256;
BEGIN
  EXECUTE IMMEDIATE 
  'BEGIN :x := tab_v256('''||REPLACE(p_string, ',', ''',''')||'''); END;'
  USING OUT ret;

  RETURN ret;
END;
/

GRANT EXECUTE ON split_string TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM split_string FOR split_string;
