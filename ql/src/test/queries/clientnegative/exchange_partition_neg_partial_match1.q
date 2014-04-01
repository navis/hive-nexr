CREATE TABLE exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING, hr STRING, mn STRING);
CREATE TABLE exchange_part_test2 (f1 string) PARTITIONED BY (hr STRING, mn STRING);

ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='1', mn='10');
ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='1', mn='20');
ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='2', mn='10');
ALTER TABLE exchange_part_test1 ADD PARTITION (ds='B', hr='2', mn='20');

ALTER TABLE exchange_part_test2 ADD PARTITION (mn='10');
ALTER TABLE exchange_part_test2 ADD PARTITION (mn='20');
ALTER TABLE exchange_part_test2 ADD PARTITION (mn='10');
ALTER TABLE exchange_part_test2 ADD PARTITION (mn='20');

-- already exists
ALTER TABLE exchange_part_test1 EXCHANGE PARTITION (ds='B', hr='2') WITH TABLE exchange_part_test2;

SHOW PARTITIONS exchange_part_test1;
SHOW PARTITIONS exchange_part_test2;
