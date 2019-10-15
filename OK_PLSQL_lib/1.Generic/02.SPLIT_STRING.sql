CREATE OR REPLACE FUNCTION split_string(p_string IN VARCHAR2, p_separator IN VARCHAR2 DEFAULT ',') RETURN tab_v256 IS
/*
  History of changes:
  ------------------------------------------------------------------------------
  06-Sep-2019, OK: re-wrote completely;
  28-Aug-2019, OK: added logic for processing NULL p_string value
*/
  ret         tab_v256;
  n_sep_len   PLS_INTEGER;
BEGIN
  IF p_string IS NULL THEN
    RETURN tab_v256();
  ELSE
    n_sep_len := LENGTH(p_separator);
    
    WITH
      qry(part, rest, sep_pos) AS
      (
        SELECT
          DECODE(sep_pos, 0, str, SUBSTR(str, 1, sep_pos-1)) part,
          DECODE(sep_pos, 0, NULL, SUBSTR(str, sep_pos + n_sep_len)) rest,
          DECODE(sep_pos, 0, 0, INSTR(SUBSTR(str, sep_pos + n_sep_len), p_separator)) sep_pos
        FROM
        (
          SELECT
            p_string AS str,
            INSTR(p_string, p_separator) AS sep_pos
          FROM dual
        )
      UNION ALL
        SELECT
          DECODE(sep_pos, 0, rest, SUBSTR(rest, 1, sep_pos-1)) part,
          DECODE(sep_pos, 0, NULL, SUBSTR(rest, sep_pos + n_sep_len)) rest,
          DECODE(sep_pos, 0, 0, INSTR(SUBSTR(rest, sep_pos + n_sep_len), p_separator)) sep_pos
        FROM qry 
        WHERE rest IS NOT NULL
      )
    SELECT part BULK COLLECT INTO ret FROM qry;
  END IF;
  
  RETURN ret;
END;
/
