CREATE TABLE hbase_partition(key int, value string) partitioned by (pid string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

alter table hbase_partition add partition (pid='100');
alter table hbase_partition add partition (pid='200');

select * from hbase_partition;

from src
insert overwrite table hbase_partition partition (pid='100') select * where key < 100
insert overwrite table hbase_partition partition (pid='200') select * where key >= 100 AND key < 200;

explain extended
select * from hbase_partition limit 10;
select * from hbase_partition limit 10;

explain extended
select * from hbase_partition where pid='100' limit 10;
select * from hbase_partition where pid='100' limit 10;

explain extended
select * from hbase_partition where pid='200' limit 10;
select * from hbase_partition where pid='200' limit 10;

drop table hbase_partition;
