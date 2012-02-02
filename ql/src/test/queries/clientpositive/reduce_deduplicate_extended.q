set hive.optimize.reducededuplication=true;
set hive.map.aggr=true;

explain select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
explain select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
explain select key, sum(key), (1000 - x) from (select key, (1000 - key) as X from src order by key) Q1 group by key, (1000 - x);
explain select key, sum(key) as value from src group by key order by key, value;
explain select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key;
explain select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key;
explain from (select key, value from src group by key, value) s select s.key group by s.key;

select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
select key, sum(key), (1000 - x) from (select key, (1000 - key) as X from src order by key) Q1 group by key, (1000 - x);
select key, sum(key) as value from src group by key order by key, value;
select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key;
select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key;
from (select key, value from src group by key, value) s select s.key group by s.key;

set hive.map.aggr=false;

explain select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
explain select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
explain select key, sum(key), (1000 - x) from (select key, (1000 - key) as X from src order by key) Q1 group by key, (1000 - x);
explain select key, sum(key) as value from src group by key order by key, value;
explain select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key;
explain select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key;
explain from (select key, value from src group by key, value) s select s.key group by s.key;

select key, sum(key) from (select * from src distribute by key sort by key, value) Q1 group by key;
select key, sum(key), lower(value) from (select * from src order by key) Q1 group by key, lower(value);
select key, sum(key), (1000 - x) from (select key, (1000 - key) as X from src order by key) Q1 group by key, (1000 - x);
select key, sum(key) as value from src group by key order by key, value;
select src.key, src.value FROM src JOIN src1 ON src.key = src1.key order by src.key;
select src.key, sum(src.key) FROM src JOIN src1 ON src.key = src1.key group by src.key;
from (select key, value from src group by key, value) s select s.key group by s.key;