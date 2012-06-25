explain select timestamp '2011-01-01 01:01:01' from src limit 1;
select timestamp '2011-01-01 01:01:01' from src limit 1;

explain select 1 from src where timestamp '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100' limit 1;
select 1 from src where timestamp '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100' limit 1;

