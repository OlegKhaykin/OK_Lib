CREATE OR REPLACE VIEW v_all_columns AS
SELECT
  owner, table_name, column_name, column_id,
  CASE
    WHEN data_type IN ('CHAR', 'VARCHAR2', 'RAW', 'NCHAR', 'NVARCHAR2') THEN data_type||'('||char_length||' '||DECODE(char_used, 'B', 'BYTE', 'C', 'CHAR')||')'
    WHEN data_type = 'NUMBER' AND data_precision IS NULL AND data_scale IS NULL THEN 'NUMBER'
    WHEN data_type = 'NUMBER' AND data_precision IS NULL AND data_scale = 0 THEN 'INTEGER'
    WHEN data_type = 'NUMBER' AND data_scale = 0 THEN 'NUMBER('||data_precision||')'
    WHEN data_type = 'NUMBER' THEN 'NUMBER('||data_precision||','||data_scale||')'
    ELSE data_type
  END data_type,
  nullable
FROM all_tab_columns;

GRANT SELECT ON v_all_columns TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM v_all_columns FOR v_all_columns;
