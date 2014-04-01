CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING);
CREATE TABLE exchange_part_test2 (f1 string);

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='1');
ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='2');

ALTER TABLE exchange_part_test1 EXCHANGE PARTITION (ds='A', hr='1') WITH TABLE exchange_part_test2;

SHOW PARTITIONS exchange_part_test1;
DESC exchange_part_test2;
