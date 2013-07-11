CREATE TABLE hbase_partition(key int, value string) partitioned by (pid string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

alter table hbase_partition add partition (pid='100');
alter table hbase_partition add partition (pid='200');

from src
insert into table hbase_partition partition (pid='100') select * where key < 100
insert into table hbase_partition partition (pid='200') select * where key >= 100 AND key < 200;

explain
select * from hbase_partition where pid='100';
select * from hbase_partition where pid='100';

explain
select * from hbase_partition where pid='200';
select * from hbase_partition where pid='200';
