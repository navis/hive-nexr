DROP TABLE hbase_table;
CREATE TABLE hbase_table(key string, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0");

INSERT OVERWRITE TABLE hbase_table SELECT 'key', 'value1' FROM src LIMIT 1;
INSERT OVERWRITE TABLE hbase_table SELECT 'key', 'value2' FROM src LIMIT 1;
INSERT OVERWRITE TABLE hbase_table SELECT 'key', 'value3' FROM src LIMIT 1;

select value from hbase_table;
select value from hbase_table('hbase.max.versions'='3');
select value from hbase_table('hbase.max.versions'='3', 'hbase.versions.order'='ASC');

-- fetch operator
select * from hbase_table('hbase.max.versions'='3');

DROP TABLE hbase_table;