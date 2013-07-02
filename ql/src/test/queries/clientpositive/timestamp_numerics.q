create table ts1 (key timestamp, value string);
create table ts2 (key timestamp, value string) STORED AS SEQUENCEFILE;
load data local inpath '../../data/files/kv1.txt' into table ts1;
load data local inpath '../../data/files/kv1.seq' into table ts2;

select * from ts1 limit 3;
select * from ts2 limit 3;
