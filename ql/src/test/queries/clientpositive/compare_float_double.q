create table test (id int, val_f float, val_d double) row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile;
load data local inpath '../data/files/floatdouble.txt' INTO TABLE test;

select val_f=-1.74f, val_f=3.14f, val_f=0f,
       val_f=-1.74d, val_f=3.14d, val_f=0d,
       val_f=-1.74, val_f=3.14, val_f=0 from test;

select val_d=123.234f, val_d=-980345.12323f, val_d=0f,
       val_d=123.234d, val_d=-980345.12323d, val_d=0d,
       val_d=123.234, val_d=-980345.12323, val_d=0 from test;

drop table test;
