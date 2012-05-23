set hive.aggresive.fetch.task.conversion=true;

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
