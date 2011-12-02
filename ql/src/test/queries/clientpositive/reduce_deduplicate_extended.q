create table t1 (int1 int, int2 int, str1 string, str2 string);
create table t2 (int1 int, int2 int, str1 string, str2 string);

set hive.map.aggr=true;

explain select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
explain select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);

select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);

set hive.map.aggr=false;

explain select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
explain select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);

select Q1.int1, sum(Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
select (Q1.int1 + 1), sum(Q1.int1 + 1) from (select * from t1 order by (int1 + 1)) Q1 group by (Q1.int1 + 1);

drop table t1;
drop table t2;