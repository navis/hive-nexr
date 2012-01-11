create table t1 (int1 int, int2 int, str1 string, str2 string);
create table t2 (int1 int, int2 int, str1 string, str2 string);

set hive.map.aggr=true;

explain select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
explain select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);
explain select (str1 + 'a') as X, str2, sum(int1), sum(int2) from t1 group by (str1 + 'a'), str2 order by X, str2;
explain select t1.str1, sum(t1.int1) FROM t1 JOIN t2 ON t1.str1 = t2.str1 group by t1.str1;

select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);
select (str1 + 'a') as X, str2, sum(int1), sum(int2) from t1 group by (str1 + 'a'), str2 order by X, str2;
select t1.str1, sum(t1.int1) FROM t1 JOIN t2 ON t1.str1 = t2.str1 group by t1.str1;

set hive.map.aggr=false;

explain select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
explain select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);
explain select (str1 + 'a') as X, str2, sum(int1), sum(int2) from t1 group by (str1 + 'a'), str2 order by X, str2;
explain select t1.str1, sum(t1.int1) FROM t1 JOIN t2 ON t1.str1 = t2.str1 group by t1.str1;

select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);
select (str1 + 'a') as X, str2, sum(int1), sum(int2) from t1 group by (str1 + 'a'), str2 order by X, str2;
select t1.str1, sum(t1.int1) FROM t1 JOIN t2 ON t1.str1 = t2.str1 group by t1.str1;

drop table t1;
drop table t2;