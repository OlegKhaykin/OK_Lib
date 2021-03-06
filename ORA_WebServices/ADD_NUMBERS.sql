CREATE OR REPLACE FUNCTION add_numbers (p_int_1  IN  NUMBER,
                                        p_int_2  IN  NUMBER)
  RETURN NUMBER
AS
  l_request   soap_api.t_request;
  l_response  soap_api.t_response;
  l_return    VARCHAR2(32767);
  
  l_url          VARCHAR2(32767);
  l_namespace    VARCHAR2(32767);
  l_method       VARCHAR2(32767);
  l_soap_action  VARCHAR2(32767);
  l_result_name  VARCHAR2(32767);
BEGIN
  l_url         := 'http://oracle-base.com/webservices/server.php';
  l_namespace   := 'xmlns="http://oracle-base.com/webservices/"';
  l_method      := 'ws_add';
  l_soap_action := 'http://oracle-base.com/webservices/server.php/ws_add';
  l_result_name := 'return';
  
  l_request := soap_api.new_request(p_method       => l_method,
                                    p_namespace    => l_namespace);

  soap_api.add_parameter(p_request => l_request,
                         p_name    => 'int1',
                         p_type    => 'xsd:integer',
                         p_value   => p_int_1);

  soap_api.add_parameter(p_request => l_request,
                         p_name    => 'int2',
                         p_type    => 'xsd:integer',
                         p_value   => p_int_2);

  l_response := soap_api.invoke(p_request => l_request,
                                p_url     => l_url,
                                p_action  => l_soap_action);

  l_return := soap_api.get_return_value(p_response  => l_response,
                                        p_name      => l_result_name,
                                        p_namespace => NULL);

  RETURN l_return;
END;
/