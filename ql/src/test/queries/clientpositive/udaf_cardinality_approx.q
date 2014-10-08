DESCRIBE FUNCTION hll;
DESCRIBE FUNCTION EXTENDED hll;

set hive.map.aggr = false;

explain
select hll(key), hll(6,key), hll(10, 20, key), hll(0.1, key) from src;

select hll(key), hll(6,key), hll(10, 20, key), hll(0.1, key) from src;

set hive.map.aggr = true;

explain
select hll(key), hll(6,key), hll(10, 20, key), hll(0.1, key) from src;

select hll(key), hll(6,key), hll(10, 20, key), hll(0.1, key) from src;
