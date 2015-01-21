drop table over10k;

create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           dec decimal,  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

select s, sum(b) over (partition by i order by s,b rows unbounded preceding) from over10k limit 100;

select s, sum(f) over (partition by d order by s,f rows unbounded preceding) from over10k limit 100;

select s, sum(f) over (partition by ts order by f range between current row and unbounded following) from over10k limit 100;

select s, avg(f) over (partition by ts order by s,f rows between current row and 5 following) from over10k limit 100;

select s, avg(d) over (partition by t order by s,d desc rows between 5 preceding and 5 following) from over10k limit 100;

select s, sum(i) over(partition by ts order by s) from over10k limit 100;

select f, sum(f) over (partition by ts order by f range between unbounded preceding and current row) from over10k limit 100;

select s, i, round(avg(d) over (partition by s order by i) / 10.0 , 2) from over10k limit 7;

select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k window w1 as (partition by s order by i) limit 7;

set hive.cbo.enable=false;
-- HIVE-9228 
select s, i from ( select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k window w1 as (partition by s order by i)) X limit 7;

-- preceding + preceding, following + following

-- streaming
select value, 
  min(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  max(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  sum(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  first_value(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  last_value(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  min(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  max(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  sum(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  first_value(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  last_value(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  min(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  max(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  sum(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  first_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  last_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
from src tablesample (10 rows);

-- non-streaming
select value, 
  min(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  max(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  sum(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  first_value(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  last_value(cast(key as int)) over (ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
  min(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  max(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  sum(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  first_value(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  last_value(cast(key as int)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
  min(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  max(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  sum(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  first_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  last_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),
  min(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
  max(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
  sum(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
  first_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
  last_value(cast(key as int)) over (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
from src tablesample (10 rows);
