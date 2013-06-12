set hive.lock.numretries=3;
set hive.lock.sleep.between.retries=3;

DROP TABLE insert_into4_neg;

CREATE TABLE insert_into4_neg (key int, value string)
  PARTITIONED BY (ds string);

INSERT INTO TABLE insert_into4_neg PARTITION (ds='1')
  SELECT * FROM src LIMIT 100;

LOCK TABLE insert_into4_neg PARTITION (ds='1') EXCLUSIVE;
INSERT INTO TABLE insert_into4_neg PARTITION (ds='1')
  SELECT * FROM src LIMIT 100;

DROP TABLE insert_into4_neg;
