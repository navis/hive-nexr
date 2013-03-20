create table allcolref_from (a string, b string, c string, d string, e string);
insert into table allcolref_from select 'a', 'b', 'c', 'd', 'e' from src tablesample (1 rows);

explain
select b... from allcolref_from;
select b... from allcolref_from;

-- table aliased
explain
select b... from allcolref_from X;
select b... from allcolref_from X;

explain
select X.b... from allcolref_from X;
select X.b... from allcolref_from X;

-- JOIN
explain
select X.b... from allcolref_from X join allcolref_from Y on X.a=Y.a;
select X.b... from allcolref_from X join allcolref_from Y on X.a=Y.a;

explain select b... from allcolref_from X join src Y on X.a=Y.key;
explain select key... from allcolref_from X join src Y on X.a=Y.key;

-- UDF
explain
select concat_ws('.', b...) from allcolref_from;
select concat_ws('.', b...) from allcolref_from;

-- UDTF
explain
select stack(2, b...) as (BC, DE) from allcolref_from;
select stack(2, b...) as (BC, DE) from allcolref_from;
