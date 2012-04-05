set hive.aggresive.fetch.task.conversion=true;

explain select key from src;
select key from src;
explain select key from src limit 10;
select key from src limit 10;
explain select key from src where key < 100 limit 10;
select key from src where key < 100 limit 10;
explain select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;
select key from srcpart where ds='2008-04-08' AND hr='11' limit 10;
