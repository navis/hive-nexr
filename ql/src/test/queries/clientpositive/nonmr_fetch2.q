set hive.fetch.task.conversion=all;

-- sub-query
explain select * from (select * from src) a join (select * from src) b on a.key=b.key;
select * from (select * from src) a join (select * from src) b on a.key=b.key;

-- union all
explain select * from (select * from src a union all select * from src b) c;
select * from (select * from src a union all select * from src b) c;

-- lateral view
EXPLAIN SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol;
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol;

-- triple union
explain select a.* from (select key from src union all select key from src union all select key from src) a;
select a.* from (select key from src union all select key from src union all select key from src) a;

-- complex
explain select a.* from (select key from src) a left outer join (select * from(select key from src union all select key from src) a)b on a.key=b.key;
select a.* from (select key from src) a left outer join (select * from(select key from src union all select key from src) a)b on a.key=b.key;
