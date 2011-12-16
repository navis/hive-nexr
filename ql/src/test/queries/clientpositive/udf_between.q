CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;

SELECT * FROM T1 where key between 2 AND 7;
SELECT * FROM T1 where key not between 2 AND 7;

SELECT * FROM T1 where 'b' between 'a' AND 'c' LIMIT 1;
SELECT * FROM T1 where 2 between 2 AND '3' LIMIT 1;

SELECT * FROM src where 100 between (50 + 30) AND 200 LIMIT 1;
SELECT * FROM src where 100 not between -50 AND 200 LIMIT 1;
