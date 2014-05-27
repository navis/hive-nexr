set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION NULLIF;
DESCRIBE FUNCTION EXTENDED NULLIF;

EXPLAIN
SELECT NULLIF(1, 1),
       NULLIF(1, 2),
       NULLIF(NULL, 2),
       NULLIF(1, NULL),
       NULLIF('1', '1'),
       NULLIF('1', '2'),
       NULLIF(NULL, '2'),
       NULLIF('1', NULL),
       NULLIF(1.0, 1.0),
       NULLIF(1.0, 2.0),
       NULLIF(NULL, 2.0),
       NULLIF(1.0, NULL),
       NULLIF(NULL, NULL),
       2 / NULLIF(0.0, 0.0)
FROM src tablesample (1 rows);

SELECT NULLIF(1, 1),
       NULLIF(1, 2),
       NULLIF(NULL, 2),
       NULLIF(1, NULL),
       NULLIF('1', '1'),
       NULLIF('1', '2'),
       NULLIF(NULL, '2'),
       NULLIF('1', NULL),
       NULLIF(1.0, 1.0),
       NULLIF(1.0, 2.0),
       NULLIF(NULL, 2.0),
       NULLIF(1.0, NULL),
       NULLIF(NULL, NULL),
       2 / NULLIF(0.0, 0.0)
FROM src tablesample (1 rows);
