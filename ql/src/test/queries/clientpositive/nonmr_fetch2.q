set hive.fetch.task.conversion=all;

-- sub-query
explain
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;

-- union all
explain
select * from (select * from src a where key < 100 union all select * from src b where key > 400 ) c;
select * from (select * from src a where key < 100 union all select * from src b where key > 400 ) c;

-- lateral view
EXPLAIN
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;

set hive.fetch.task.conversion.list.fetch=true;

-- sub-query
explain
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;

-- union all
explain
select * from src a where key < 50 union all select * from src b where key > 450;
select * from src a where key < 50 union all select * from src b where key > 450;

-- lateral view
EXPLAIN
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;
