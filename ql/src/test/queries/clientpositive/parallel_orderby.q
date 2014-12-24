create table src5 (key string, value string);
load data local inpath '../../data/files/kv5.txt' into table src5;
load data local inpath '../../data/files/kv5.txt' into table src5;

set mapred.reduce.tasks = 4;
set hive.optimize.sampling.orderby=true;
set hive.optimize.sampling.orderby.percent=0.66f;

explain
create table total_ordered as select * from src5 order by key, value;
create table total_ordered as select * from src5 order by key, value;

desc formatted total_ordered;
select * from total_ordered;

set hive.optimize.sampling.orderby.percent=0.0001f;
-- rolling back to single task in case that the number of sample is not enough

drop table total_ordered;
create table total_ordered as select * from src5 order by key, value;

desc formatted total_ordered;
select * from total_ordered;

set hive.optimize.sampling.orderby.percent=0.66f;
set hive.optimize.sampling.orderby.split.per.input=3;

explain
select * from src5 order by key, value;
select * from src5 order by key, value;

-- partitioned table 
explain
select * from srcpart order by key, value;
select * from srcpart order by key, value;

