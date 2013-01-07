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
