CREATE OR REPLACE PACKAGE BODY pkg_study AS
  TYPE typ_string_list IS TABLE OF VARCHAR2(256) INDEX BY VARCHAR2(30);
  
  lst_parameters  typ_string_list;

  PROCEDURE set_parameter(p_name IN VARCHAR2, p_value IN VARCHAR2) IS
  BEGIN
    lst_parameters(p_name) := p_value;
  END;
  
  FUNCTION get_parameter(p_name IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN lst_parameters(p_name);
  END;
END pkg_study;
/
