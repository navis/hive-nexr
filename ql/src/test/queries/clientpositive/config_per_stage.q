set stage-1.mapred.reduce.tasks=2;
set stage-2.mapred.reduce.tasks=3;

-- HIVE-3946 Make it possible to configure for each stage
explain select * from (select * from src sort by key) a sort by value;
select * from (select * from src sort by key) a sort by value;
