create table TBL (a string, b string);
insert into table TBL select 'a','a' from src tablesample (1 rows);

set hive.optimize.correlation=true;

explain
select b, sum(cc) from (
        select b,count(1) as cc from TBL group by b
        union all
        select a as b,count(1) as cc from TBL group by a
) z
group by b;

select b, sum(cc) from (
        select b,count(1) as cc from TBL group by b
        union all
        select a as b,count(1) as cc from TBL group by a
) z
group by b;
