CREATE OR REPLACE FUNCTION eval_date(i_string IN VARCHAR2) RETURN DATE AS
  ret DATE;
BEGIN
  EXECUTE IMMEDIATE 'BEGIN :ret := '||CASE WHEN REGEXP_LIKE(i_string, 'DATE', 'i') THEN i_string ELSE 'TO_DATE('''||i_string||''')' END ||'; END;' USING OUT ret;
  RETURN ret;
END;
/
 
GRANT EXECUTE ON eval_date TO PUBLIC;
