CREATE TEMPORARY FUNCTION col_name_udf AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFColumnNameTest';
CREATE TEMPORARY FUNCTION col_name_udtf AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFColumnNameTest';
CREATE TEMPORARY FUNCTION col_name_udaf AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFColumnNameTest';

explain
select col_name_udf(key, key + value, cast(value as int), value + 1) from src TABLESAMPLE (10 ROWS);
select col_name_udf(key, key + value, cast(value as int), value + 1) from src TABLESAMPLE (10 ROWS);

explain
select col_name_udtf(key, key + value, cast(value as int), value + 1) AS (a, b, c, d) from src TABLESAMPLE (10 ROWS);
select col_name_udtf(key, key + value, cast(value as int), value + 1) AS (a, b, c, d) from src TABLESAMPLE (10 ROWS);

explain
select col_name_udaf(key, key + value, cast(value as int), value + 1) from src TABLESAMPLE (10 ROWS) group by key;
select col_name_udaf(key, key + value, cast(value as int), value + 1) from src TABLESAMPLE (10 ROWS) group by key;
