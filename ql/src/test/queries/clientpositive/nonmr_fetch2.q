set hive.fetch.task.conversion=all;

-- sub-query
explain
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;

-- union all
explain
select * from src a where key < 50 union all select * from src b where key > 450 union all select * from src1;
select * from src a where key < 50 union all select * from src b where key > 450 union all select * from src1;

-- lateral view
EXPLAIN
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;

-- complex
explain
select a.* from (select key from src where key > 490) a left outer join (select * from(select key from src union all select key from src) a where key > 450)b on a.key=b.key;
select a.* from (select key from src where key > 490) a left outer join (select * from(select key from src union all select key from src) a where key > 450)b on a.key=b.key;

set hive.fetch.task.conversion.list.fetch=true;

-- sub-query
explain
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;
select * from (select * from src where key > 450) a join (select * from src where key > 450) b on a.key=b.key;

-- union all
explain
select * from src a where key < 50 union all select * from src b where key > 450 union all select * from src1;
select * from src a where key < 50 union all select * from src b where key > 450 union all select * from src1;

-- lateral view
EXPLAIN
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;
SELECT * FROM src LATERAL VIEW explode(array(key, value)) myTable AS myCol where key > 450;

-- complex
explain
select a.* from (select key from src where key > 490) a left outer join (select * from(select key from src union all select key from src) a where key > 450)b on a.key=b.key;
select a.* from (select key from src where key > 490) a left outer join (select * from(select key from src union all select key from src) a where key > 450)b on a.key=b.key;

set hive.fetch.task.conversion.insert=true;

-- CTAS
explain
create table xy as select * from src order by key limit 100;
create table xy as select * from src order by key limit 100;
select * from xy;

-- insert
explain
insert overwrite table xy select * from src order by key limit 100;
insert overwrite table xy select * from src order by key limit 100;
select * from xy;

