create table ppr_test (key string) partitioned by (ds string);

alter table ppr_test add partition (ds = '1234');
alter table ppr_test add partition (ds = '1224');
alter table ppr_test add partition (ds = '1214');
alter table ppr_test add partition (ds = '12+4');
alter table ppr_test add partition (ds = '12.4');
alter table ppr_test add partition (ds = '12:4');
alter table ppr_test add partition (ds = '12%4');
alter table ppr_test add partition (ds = '12*4');

insert overwrite table ppr_test partition(ds = '1234') select explode(array('abcd', '1234')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '1224') select explode(array('abcd', '1224')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '1214') select explode(array('abcd', '1214')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '12+4') select explode(array('abcd', '12+4')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '12.4') select explode(array('abcd', '12.4')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '12:4') select explode(array('abcd', '12:4')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '12%4') select explode(array('abcd', '12%4')) as col from src limit 2;
insert overwrite table ppr_test partition(ds = '12*4') select explode(array('abcd', '12*4')) as col from src limit 2;


select * from ppr_test where ds = '1234' order by key;
select * from ppr_test where ds = '1224' order by key;
select * from ppr_test where ds = '1214' order by key;
select * from ppr_test where ds = '12.4' order by key;
select * from ppr_test where ds = '12+4' order by key;
select * from ppr_test where ds = '12:4' order by key;
select * from ppr_test where ds = '12%4' order by key;
select * from ppr_test where ds = '12*4' order by key;
select * from ppr_test where ds = '12.*4' order by key;

select * from ppr_test where ds = '1234' and key = '1234';
select * from ppr_test where ds = '1224' and key = '1224';
select * from ppr_test where ds = '1214' and key = '1214';
select * from ppr_test where ds = '12.4' and key = '12.4';
select * from ppr_test where ds = '12+4' and key = '12+4';
select * from ppr_test where ds = '12:4' and key = '12:4';
select * from ppr_test where ds = '12%4' and key = '12%4';
select * from ppr_test where ds = '12*4' and key = '12*4';


