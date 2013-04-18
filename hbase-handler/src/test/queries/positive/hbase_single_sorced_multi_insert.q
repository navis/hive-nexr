CREATE TABLE src_x1(key string, value string);
CREATE TABLE src_x2(key string, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

explain
from src a
insert overwrite table src_x1
select key,value where a.key > 0 AND a.key < 50
insert overwrite table src_x2
select key,value where a.key > 50 AND a.key < 100;

from src a
insert overwrite table src_x1
select key,value where a.key > 0 AND a.key < 50
insert overwrite table src_x2
select key,value where a.key > 50 AND a.key < 100;

select * from src_x1;
select * from src_x2;
