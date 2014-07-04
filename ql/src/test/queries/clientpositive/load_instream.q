create table kv_temp (key string, value string) ROW FORMAT delimited fields terminated by ' ' STORED AS TEXTFILE;

load data local instream 'key value\nkey2 value2' into table kv_temp;

select * from kv_temp;
