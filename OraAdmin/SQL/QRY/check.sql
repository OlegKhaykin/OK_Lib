SELECT 'Number of Java objects: ' || To_Char(Count(*))
FROM user_objects
WHERE object_type LIKE 'JAVA %';

SELECT 'Number of Java objects: ' || To_Char(Count(*))
FROM user_objects
WHERE object_type LIKE 'JAVA %' AND status <> 'VALID'
