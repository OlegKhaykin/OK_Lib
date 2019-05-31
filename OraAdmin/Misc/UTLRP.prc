CREATE OR REPLACE PROCEDURE UTLRP(p_owner IN VARCHAR2 DEFAULT NULL) AUTHID CURRENT_USER AS
  cnt   PLS_INTEGER;
  cmd   VARCHAR2(1000);
BEGIN
  LOOP
    cnt := 0;

    FOR r IN
    (
      SELECT
        owner,
        Decode(object_type, 'PACKAGE BODY', 'PACKAGE', object_type) object_type,
        object_name,
        Decode(object_type, 'PACKAGE BODY', 'COMPILE BODY', 'COMPILE') cmd
      FROM dba_objects
      WHERE object_type IN ('PACKAGE BODY','FUNCTION','PROCEDURE','TRIGGER','VIEW', 'MATERIALIZED VIEW')
      AND status <> 'VALID'
      AND owner = NVL(p_owner, owner)
    )
    LOOP
      BEGIN
        EXECUTE IMMEDIATE 'ALTER '||r.object_type||' '||r.owner||'.'||r.object_name||' '||r.cmd;
        DBMS_OUTPUT.PUT_LINE(r.object_type||' '||r.owner||'.'||r.object_name||': compiled SUCCESSFULLY');
        cnt := cnt+1;
      EXCEPTION
       WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(r.object_type||' '||r.owner||'.'||r.object_name||': has errors');
      END;
    END LOOP;
    IF cnt = 0 THEN EXIT; END IF;
  END LOOP;

  FOR r IN
  (
    SELECT s.owner, s.synonym_name, r.owner object_owner, r.object_name
    FROM dba_objects o, dba_synonyms s, dba_objects r
    WHERE o.object_type = 'SYNONYM' AND o.status <> 'VALID'
    AND s.owner = o.owner AND s.synonym_name = o.object_name
    AND r.owner(+) = s.table_owner AND r.object_name(+) = s.table_name
  )
  LOOP
    cmd := 'DROP '||CASE r.owner WHEN 'PUBLIC' THEN 'PUBLIC' END || ' SYNONYM ' || CASE r.owner WHEN 'PUBLIC' THEN '' ELSE r.owner||'.' END ||r.synonym_name;
    EXECUTE IMMEDIATE cmd;

    IF r.object_owner IS NOT NULL THEN
      CMD := 'CREATE '||CASE r.owner WHEN 'PUBLIC' THEN 'PUBLIC' END || ' SYNONYM ' || CASE r.owner WHEN 'PUBLIC' THEN '' ELSE r.owner||'.' END ||r.synonym_name||' FOR '|| r.object_owner||'.'||r.object_name;
      EXECUTE IMMEDIATE CMD;
    END IF;
    DBMS_OUTPUT.PUT_LINE('Synonym '||r.synonym_name||' re-created');
  END LOOP;
END;
/

GRANT EXECUTE ON utlrp TO public;

CREATE PUBLIC SYNONYM utlrp FOR utlrp;
