CREATE OR REPLACE FUNCTION eval_boolean(i_string IN VARCHAR2) RETURN BOOLEAN AS
  v_flag  CHAR(1);
  ret     BOOLEAN;
BEGIN
  EXECUTE IMMEDIATE '
  DECLARE
    b_flag BOOLEAN;
  BEGIN
    b_flag := '||i_string||';
    IF b_flag THEN :v_flag := ''Y'';
    ELSIF NOT b_flag THEN :v_flag := ''N'';
    END IF;
  END;'
  USING OUT v_flag;

  RETURN CASE v_flag WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END;
END;
/

GRANT EXECUTE ON eval_boolean TO PUBLIC;
