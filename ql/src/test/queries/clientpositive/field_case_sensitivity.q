drop table tabletxt;
drop table tableorc;
drop table tablerc;

CREATE TABLE tabletxt (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';
LOAD DATA LOCAL INPATH '../../data/files/struct.txt' INTO TABLE tabletxt;

CREATE TABLE tablerc (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) STORED AS RCFILE;

CREATE TABLE tableorc (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) STORED AS ORC;

FROM tabletxt
INSERT OVERWRITE TABLE tablerc SELECT *
INSERT OVERWRITE TABLE tableorc SELECT *;

select * from tabletxt;
select strct from tabletxt;
select strct.a from tabletxt;
select strct.A from tabletxt;

select * from tablerc;
select strct from tablerc;
select strct.a from tablerc;
select strct.A from tablerc;

select * from tableorc;
select strct from tableorc;
select strct.a from tableorc;
select strct.A from tableorc;
