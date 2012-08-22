CREATE TABLE hbase_sampling (key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

INSERT OVERWRITE TABLE hbase_sampling SELECT * FROM src;

-- percent sampling
explain
select * from hbase_sampling TABLESAMPLE (0.25 PERCENT);
select * from hbase_sampling TABLESAMPLE (0.25 PERCENT);

explain
select * from hbase_sampling TABLESAMPLE (1 PERCENT);
select * from hbase_sampling TABLESAMPLE (1 PERCENT);
