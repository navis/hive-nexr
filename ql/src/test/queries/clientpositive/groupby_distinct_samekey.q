explain select Q1.key, sum(distinct Q1.key) from (select * from src order by key) Q1 group by Q1.key;
explain select key, sum(distinct key) from src group by key;

select Q1.key, sum(distinct Q1.key) from (select * from src order by key) Q1 group by Q1.key;
select key, sum(distinct key) from src group by key;
