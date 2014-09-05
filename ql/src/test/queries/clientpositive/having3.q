explain
select value,max(key)-min(key) as span from src tablesample (10 rows) group by value having span>=0;

select value,max(key)-min(key) as span from src tablesample (10 rows) group by value having span>=0;
