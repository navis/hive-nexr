set hive.aggresive.fetch.task.conversion=true;

-- backward
explain select * from src limit 10;
select * from src limit 10;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- limit
explain select key from src limit 10;
select key from src limit 10;

-- filter
explain select key from src where key < 100 limit 10;
select key from src where key < 100 limit 10;

-- partitioned
explain select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- virtual columns
explain select *, BLOCK__OFFSET__INSIDE__FILE from src where key < 10 limit 10;
select *, BLOCK__OFFSET__INSIDE__FILE from src where key < 100 limit 10;

-- virtual columns on partitioned
explain select *, BLOCK__OFFSET__INSIDE__FILE from srcpart where key < 10 limit 30;
select *, BLOCK__OFFSET__INSIDE__FILE from srcpart where key < 10 limit 30;

-- bucket sampling
explain select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);
select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (BUCKET 1 OUT OF 40 ON key);

-- non deterministic
explain select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;
select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;

-- not yet
explain select * from srcpart TABLESAMPLE (1 PERCENT);

