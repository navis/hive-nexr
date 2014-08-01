set hive.fetch.task.conversion=more;

create temporary function os as 'org.apache.hadoop.hive.ql.udf.UDFObjectToString';
create temporary function tv as 'org.apache.hadoop.hive.ql.udf.UDFTypeVariable';

explain
select os(key), os(cast(key as smallint)), os(cast(key as int)), os(cast(key as float)), os(cast(key as double)), os(cast(key as bigint)) from src tablesample (1 rows);
select os(key), os(cast(key as smallint)), os(cast(key as int)), os(cast(key as float)), os(cast(key as double)), os(cast(key as bigint)) from src tablesample (1 rows);

explain
select tv(key), tv(cast(key as smallint)), tv(cast(key as int)), tv(cast(key as float)), tv(cast(key as double)), tv(cast(key as bigint)) from src tablesample (1 rows);
select tv(key), tv(cast(key as smallint)), tv(cast(key as int)), tv(cast(key as float)), tv(cast(key as double)), tv(cast(key as bigint)) from src tablesample (1 rows);

explain
select tv(map(value, key)), tv(map(value, cast(key as smallint))), tv(map(value, cast(key as int))), tv(map(value, cast(key as float))), tv(map(value, cast(key as double))),tv(map(value, cast(key as bigint))) from src tablesample (1 rows);
select tv(map(value, key)), tv(map(value, cast(key as smallint))), tv(map(value, cast(key as int))), tv(map(value, cast(key as float))), tv(map(value, cast(key as double))),tv(map(value, cast(key as bigint))) from src tablesample (1 rows);
