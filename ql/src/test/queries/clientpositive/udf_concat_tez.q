set hive.vectorized.execution.enabled=true;

create table lineitem (l_orderkey bigint, l_linestatus string) STORED AS ORC;
insert overwrite table lineitem select * from src tablesample (10 rows);

explain
SELECT concat(l_orderkey, l_linestatus) FROM lineitem;
SELECT concat(l_orderkey, l_linestatus) FROM lineitem;
