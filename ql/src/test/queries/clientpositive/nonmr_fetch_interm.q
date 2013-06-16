set hive.auto.convert.join=false;
set hive.fetch.task.conversion.interm=true;
set hive.fetch.task.conversion.threshold=5000;

explain
select * from src a left semi join src b on a.key=b.key + 300 order by a.value;
select * from src a left semi join src b on a.key=b.key + 300 order by a.value;
