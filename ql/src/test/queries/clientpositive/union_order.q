select * from (select '1234' from src limit 1 union all select 'abcd' from src limit 1) s;
select * from (select 'abcd' from src limit 1 union all select '1234' from src limit 1) s;
