CREATE OR REPLACE PROCEDURE kill_session(p_sid IN NUMBER, p_serial IN NUMBER) AS
  v_username    VARCHAR2(30);
  c_is_system   CHAR(1);
BEGIN
  SELECT u.username, u.oracle_maintained INTO v_username, c_is_system
  FROM gv$session s
  JOIN dba_users u ON u.username = s.username
  WHERE sid = p_sid AND serial# = p_serial;

  IF c_is_system = 'Y' THEN
    Raise_Application_Error(-20000, 'Not allowed to kill sessions of Oracle-maintained users; USERNAME: '||v_username);
  END IF;
   
  EXECUTE IMMEDIATE 'ALTER SYSTEM DISCONNECT SESSION '''||p_sid||','||p_serial||''' IMMEDIATE';
EXCEPTION
 WHEN NO_DATA_FOUND THEN
  Raise_Application_Error(-20000, 'No such session: SID='||p_sid||', SERIAL#='||p_serial);
END;
/

GRANT EXECUTE ON kill_session TO deployer;