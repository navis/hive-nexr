create table t1( key_int1 int, key_int2 int, key_string1 string, key_string2 string);

select key_int1, sum(distinct key_int1) from t1 group by key_int1;

DROP TABLE t1;