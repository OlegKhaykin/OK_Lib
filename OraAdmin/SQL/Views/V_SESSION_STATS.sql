prompt Creating view V_SESSION_STATS

CREATE OR REPLACE VIEW v_session_stats AS
SELECT status, username, machine, process, count(*) sessions
FROM v$session
GROUP BY status, username , machine, process;

GRANT SELECT ON v_session_stats TO dba;
CREATE OR REPLACE PUBLIC SYNONYM v_session_stats FOR v_session_stats;

CREATE TABLE tst_session_stats AS SELECT SYSDATE dtime, v.* FROM v_session_stats v;
GRANT ALL ON tst_session_stats TO dba;
CREATE OR REPLACE PUBLIC SYNONYM tst_session_stats FOR tst_session_stats;

CREATE INDEX idx_sess_stats ON tst_session_stats(dtime);