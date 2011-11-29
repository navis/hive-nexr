dfs -put ../data/files/name-phone.txt tmp/dpal-129/p/dt=20110901/data/a.data;
dfs -put ../data/files/name-phone.txt tmp/dpal-129/p/dt=20110902/data/a.data;

dfs -mkdir tmp/dpal-129/q/dt=20110901;
dfs -put ../data/files/name-phone.txt tmp/dpal-129/q/dt=20110902/data/a.data;
dfs -put ../data/files/name-phone.txt tmp/dpal-129/q/dt=20110903/data/a.data;

drop table p;
drop table q;

create external table p (name string, phone string) partitioned by (dt string)
  row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile
  location 'tmp/dpal-129/p/{**/*.data}';

create external table q (name string, phone string) partitioned by (dt string)
  row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile
  location 'tmp/dpal-129/q{/**/*.data}';

ALTER TABLE p ADD PARTITION (dt = '20110901');
ALTER TABLE p ADD PARTITION (dt = '20110902');

ALTER TABLE q ADD PARTITION (dt = '20110901');
ALTER TABLE q ADD PARTITION (dt = '20110902');
ALTER TABLE q ADD PARTITION (dt = '20110903');

select name from p;
select name from q;

select * from p join q on p.name=q.name;

dfs -rmr tmp/dpal-129;