prompt Creating procedure SP_EXEC_CMD

CREATE OR REPLACE PROCEDURE sp_exec_cmd(p_cmd IN VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE p_cmd;
END;
/
