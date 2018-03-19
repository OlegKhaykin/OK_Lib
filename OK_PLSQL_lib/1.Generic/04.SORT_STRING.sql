CREATE OR REPLACE FUNCTION sort_string(p_string IN VARCHAR2) RETURN VARCHAR2 AS
  len     PLS_INTEGER;
  tab     tab_v256;
  ret     VARCHAR2(256);
BEGIN
  len := LENGTH(p_string);
  IF len > 256 THEN
    Raise_Application_Error(-20000,'The string is too long. It should not have more than 256 characters.');
  END IF;
  tab := tab_v256();
  tab.EXTEND(len);
  FOR i IN 1..len LOOP tab(i) := SUBSTR(p_string, i, 1); END LOOP;
  FOR r IN (SELECT VALUE(t) c FROM TABLE(CAST(tab AS tab_v256)) t ORDER BY VALUE(t)) LOOP
    ret := ret || r.c;
  END LOOP;
  
  RETURN ret;
END;
/

GRANT EXECUTE ON sort_string TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM sort_string FOR sort_string;