create table src_rc (key string, value string) stored as rcfile;

SET io.seqfile.compression.type=BLOCK;
SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;

insert into table src_rc select * from src;
select * from src_rc tablesample (10 rows);
