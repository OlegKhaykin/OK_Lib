SET sql_mode=ORACLE;

DELIMITER //

CREATE OR REPLACE PACKAGE xl AS
  PROCEDURE open_log(p_name IN VARCHAR, p_comment IN VARCHAR, p_debug BOOLEAN);
 
  FUNCTION get_current_proc_id RETURN SMALLINT UNSIGNED;
 
  PROCEDURE begin_action
  (
    p_action    IN VARCHAR,
    p_comment   IN VARCHAR,
    p_debug     IN BIT
  );
 
  PROCEDURE end_action(p_comment IN VARCHAR);
 
  PROCEDURE close_log(p_result IN VARCHAR, p_dump IN BOOLEAN);
  
END;
//


DELIMITER ;

GRANT ALL ON xl TO everybody;
