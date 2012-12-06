set hive.exec.reducers.bytes.per.reducer=600;

-- SORT_AND_HASH_QUERY_RESULTS

-- simple (one partition for each cluster)
explain select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0, b.key = 100, CAST(a.key as int) > 300);

select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0, b.key = 100, CAST(a.key as int) > 300);

-- percent based assigning
explain select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);

select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);

-- subquery
explain select count(*) from (select distinct key from src) a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);

select count(*) from (select distinct key from src) a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);

-- nested skewjoin
explain
select * from
   (select a.* from src a JOIN src b ON a.key=b.key SKEWED ON (
      a.key < 10 CLUSTER BY 20 PERCENT,
      b.key < 20 CLUSTER BY 20 PERCENT) where a.key < 70) X
   JOIN
   (select a.* from src a JOIN src b ON a.key=b.key SKEWED ON (
      a.key < 60 CLUSTER BY 20 PERCENT,
      b.key < 70 CLUSTER BY 20 PERCENT) where a.key > 50) Y
   ON X.key=Y.key SKEWED ON (
      X.key < 55 CLUSTER BY 20 PERCENT,
      Y.key > 65 CLUSTER BY 20 PERCENT);

select * from
   (select a.* from src a JOIN src b ON a.key=b.key SKEWED ON (
      a.key < 10 CLUSTER BY 20 PERCENT,
      b.key < 20 CLUSTER BY 20 PERCENT) where a.key < 70) X
   JOIN
   (select a.* from src a JOIN src b ON a.key=b.key SKEWED ON (
      a.key < 60 CLUSTER BY 20 PERCENT,
      b.key < 70 CLUSTER BY 20 PERCENT) where a.key > 50) Y
   ON X.key=Y.key SKEWED ON (
      X.key < 55 CLUSTER BY 20 PERCENT,
      Y.key > 65 CLUSTER BY 20 PERCENT);

set hive.auto.convert.join = true;
set hive.auto.convert.join.noconditionaltask = false;

-- auto-convert (map-join)
explain
select X.* from
   (select * from src where key < 70) X
   JOIN
   (select * from src where key > 50) Y
   ON X.key=Y.key SKEWED ON (
      X.key < 55 CLUSTER BY 20 PERCENT,
      Y.key > 65 CLUSTER BY 20 PERCENT) order by X.key;

-- auto-convert (map-join)
select X.* from
   (select * from src where key < 70) X
   JOIN
   (select * from src where key > 50) Y
   ON X.key=Y.key SKEWED ON (
      X.key < 55 CLUSTER BY 20 PERCENT,
      Y.key > 65 CLUSTER BY 20 PERCENT) order by X.key;

set hive.mapjoin.smalltable.filesize=100;

-- auto-convert (common-join)
select X.* from
   (select * from src where key < 70) X
   JOIN
   (select * from src where key > 50) Y
   ON X.key=Y.key SKEWED ON (
      X.key < 55 CLUSTER BY 20 PERCENT,
      Y.key > 65 CLUSTER BY 20 PERCENT) order by X.key;

reset;
set hive.exec.reducers.bytes.per.reducer=3000;

-- negative, 3 reducers for 5 groups (4 skewed +1 non-skewed), disabled at runtime
explain select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 10 PERCENT,
   a.key = 10 CLUSTER BY 10 PERCENT,
   a.key = 20 CLUSTER BY 10 PERCENT,
   b.key = 30 CLUSTER BY 10 PERCENT);

select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 10 PERCENT,
   a.key = 10 CLUSTER BY 10 PERCENT,
   a.key = 20 CLUSTER BY 10 PERCENT,
   b.key = 30 CLUSTER BY 10 PERCENT);
