set hive.lock.numretries=3;
set hive.lock.sleep.between.retries=3;

DROP TABLE insert_into2_neg;

CREATE TABLE insert_into2_neg (key int, value string);

LOCK TABLE insert_into2_neg EXCLUSIVE;
INSERT INTO TABLE insert_into2_neg SELECT * FROM src LIMIT 100;

DROP TABLE insert_into2_neg;
