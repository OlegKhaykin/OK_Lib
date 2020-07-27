CREATE OR REPLACE PACKAGE pkg_study AS
  PROCEDURE set_parameter(p_name IN VARCHAR2, p_value IN VARCHAR2);
  
  FUNCTION get_parameter(p_name IN VARCHAR2) RETURN VARCHAR2;
END pkg_study;
/
