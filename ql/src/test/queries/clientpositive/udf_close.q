set user.defined=positive;

create temporary function close_test as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestClose';

select close_test(key) from src limit 10;