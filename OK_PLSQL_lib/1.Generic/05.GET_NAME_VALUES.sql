CREATE OR REPLACE FUNCTION get_name_values(i_str IN VARCHAR2) RETURN tab_name_values AS
  ret tab_name_values;
BEGIN
/*
  You can select from this function for example like this:
 
  SELECT * FROM TABLE(get_name_values('RevalDate=08/28/2015, Dataset=Official, Scenario=Base'));
*/
  SELECT obj_name_value(name, value) BULK COLLECT INTO ret
  FROM
  (
    SELECT TRIM(SUBSTR(line, 1, eq-1)) name, TRIM(SUBSTR(line, eq+1)) value
    FROM
    (
      SELECT VALUE(t) line, INSTR(VALUE(t), '=') eq
      FROM TABLE(split_string(i_str)) t
    )
  );
 
  RETURN ret;
END;
/

GRANT EXECUTE ON get_name_values TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM get_name_values FOR get_name_values;
 