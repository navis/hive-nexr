EXPLAIN SELECT value FROM SRC ORDER BY key desc limit 10;
SELECT value FROM SRC ORDER BY key desc limit 10;

EXPLAIN SELECT value FROM SRC DISTRIBUTE BY value SORT BY key desc limit 10;
SELECT value FROM SRC DISTRIBUTE BY value SORT BY key desc limit 10;
