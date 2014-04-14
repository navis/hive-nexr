set hive.lock.numretries=0;
set hive.unlock.numretries=0;

create database lockneg1;
use lockneg1;

create table tstsrcpart like default.srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from default.srcpart where ds='2008-04-08' and hr='11';

lock database lockneg1 shared;
show locks;

select count(1) from tstsrcpart where ds='2008-04-08' and hr='11';

unlock database lockneg1;
show locks;

lock database lockneg1 exclusive;
show locks;

select count(1) from tstsrcpart where ds='2008-04-08' and hr='11';
