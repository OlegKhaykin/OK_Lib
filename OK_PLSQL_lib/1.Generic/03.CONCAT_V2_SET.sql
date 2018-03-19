CREATE OR REPLACE FUNCTION concat_v2_set
(
  p_cur IN SYS_REFCURSOR,
  p_sep IN VARCHAR2 DEFAULT ','
) RETURN CLOB 
  AUTHID CURRENT_USER
AS
  -- 14-Sep-2016, OK: increased size of VAL up to 3900
  val  VARCHAR2(3900);
  ret  CLOB;
BEGIN
  LOOP
    FETCH p_cur INTO val;
    EXIT WHEN p_cur%NOTFOUND;

    ret := ret || val || p_sep;
  END LOOP;
  CLOSE p_cur;
  RETURN SUBSTR(ret, 1, LENGTH(ret) - LENGTH(p_sep));
END;
/

GRANT EXECUTE ON concat_v2_set TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM concat_v2_set FOR concat_v2_set; 