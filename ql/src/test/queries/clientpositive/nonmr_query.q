set hive.nomr.conversion=true;
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

-- split sampling
explain select * from srcpart TABLESAMPLE (0.25 PERCENT);
select * from srcpart TABLESAMPLE (0.25 PERCENT);
explain select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (0.25 PERCENT);
select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart TABLESAMPLE (0.25 PERCENT);

-- non deterministic
explain select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;
select key, value, BLOCK__OFFSET__INSIDE__FILE from srcpart where ds="2008-04-09" AND rand() > 1;

explain select key from src order by key desc limit 10;
select key, value from src order by key desc limit 10;

explain select key, value from src order by key desc limit 10;
select key, value from src order by key desc limit 10;

explain select src.key from src join src src2 on src.key=src2.key where src2.key < 10 order by key;
select src.key from src join src src2 on src.key=src2.key where src2.key < 10 order by key;

explain select * from src src1 join src src2 on src1.key=src2.key order by src1.key desc limit 10;
select * from src src1 join src src2 on src1.key=src2.key order by src1.key desc limit 10;

explain select sum(cast (src1.key as int)) as keysum from src src1 join src src2 on src1.key=src2.key group by src1.key order by keysum desc limit 10;
select sum(cast (src1.key as int)) as keysum from src src1 join src src2 on src1.key=src2.key group by src1.key order by keysum desc limit 10;
