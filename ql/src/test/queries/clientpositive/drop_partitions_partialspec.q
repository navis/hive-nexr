create table part (key string, value string) partitioned by (ds string, hr string);
alter table part add partition (ds='2008-04-08', hr='11');
alter table part add partition (ds='2008-04-08', hr='12');
alter table part add partition (ds='2008-04-09', hr='11');
alter table part add partition (ds='2008-04-09', hr='12');

show partitions part;

explain
alter table part drop partition (ds='2008-04-09');
alter table part drop partition (ds='2008-04-09');

show partitions part;

-- does not throw exception for partial spec
alter table part drop partition (ds='2008-04-09');
