CREATE OR REPLACE FUNCTION add_rest_numbers(p_int_1 IN NUMBER, p_int_2  IN  NUMBER)
  RETURN NUMBER
AS
  l_clob    CLOB;
  l_result  VARCHAR2(32767);
BEGIN
  l_clob := apex_web_service.make_rest_request
  (
    p_url         => 'http://oracle-base.com/webservices/add-numbers.php',
    p_http_method => 'GET',
    p_parm_name   => apex_util.string_to_table('p_int_1:p_int_2'),
    p_parm_value  => apex_util.string_to_table(p_int_1 || ':' || p_int_2)
  );

  -- Display the whole SOAP document returned.
  DBMS_OUTPUT.put_line('l_clob=' || l_clob);

  -- Pull out the specific value of interest.
  l_result := apex_web_service.parse_xml
  (
    p_xml   => XMLTYPE(l_clob),
    p_xpath => '//answer/number/text()'
  );

  DBMS_OUTPUT.put_line('l_result=' || l_result);

  RETURN TO_NUMBER(l_result);
END;
/
