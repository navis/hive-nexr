create table impressions (imp string, msg string)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/in7.txt' INTO TABLE impressions;

select * from impressions;

select imp,msg from impressions;

drop table impressions;

create table loc2 ( 
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '||' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/loc2.txt' INTO TABLE loc2;

select * from loc2;

drop table loc2;
