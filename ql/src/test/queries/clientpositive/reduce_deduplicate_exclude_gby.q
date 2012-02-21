set hive.optimize.reducededuplication=true;

set hive.map.aggr=false;
select * from (select * from src distribute by key sort by key) a group by a.key limit 1;

select /*+ MAPJOIN(a) */ * from (select * from src distribute by key sort by key) a join src b on a.key = b.key limit 1;

set hive.auto.convert.join=true;
select * from (select * from src distribute by key sort by key) a join src b on a.key = b.key limit 1;

