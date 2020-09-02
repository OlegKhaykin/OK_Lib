BEGIN
  FOR r IN
  (
    SELECT
      o.owner, o.object_name, o.synonym_name, o.username
    FROM
    (
      SELECT
        owner, object_type, object_name,
        CASE object_name WHEN 'PKG_DBG_XLOGGER' THEN 'XL' ELSE object_name END synonym_name,
        SYS_CONTEXT('USERENV','CURRENT_SCHEMA') username
      FROM all_objects
      WHERE owner = 'DEBUGER'
      AND object_type IN ('TABLE', 'PACKAGE')
      AND (object_name LIKE 'DBG%' OR object_name = 'PKG_DBG_XLOGGER')
    ) o
    LEFT JOIN all_synonyms s
      ON s.owner = o.username AND s.synonym_name = o.synonym_name
    WHERE s.owner IS NULL
  )
  LOOP
    EXECUTE IMMEDIATE 'CREATE OR REPLACE PUBLIC SYNONYM '||r.username||'.'||r.synonym_name||' FOR '||r.owner||'.'||r.object_name;
  END LOOP;
END;
/