create table src_100 (key string, value string);
insert into table src_100 select * from src limit 100;

set hive.parallel.orderby.bucketing.num = 3;

explain extended select key,value from src_100 order by key;
select key,value from src_100 order by key;

explain extended select sum(key) as sum, value from src_100 group by value order by sum;
select sum(key) as sum, value from src_100 group by value order by sum;

-- negative, subquery
explain extended select sum(key), a.value from (select * from src_100 order by key) a group by a.value;

-- negative, insert
CREATE TABLE insert_temp (key int, value string);

EXPLAIN extended INSERT INTO TABLE insert_temp SELECT sum(key) as sum, value from src_100 group by value order by sum;

