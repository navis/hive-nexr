




CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;
LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;
LOAD DATA LOCAL INPATH '../data/files/T3.txt' INTO TABLE T3;
LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T4;

EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;


EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON a.val = c.val
          JOIN T4 d ON a.key + 1 = d.key + 1;


SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON a.val = c.val
          JOIN T4 d ON a.key + 1 = d.key + 1;


set hive.auto.convert.join=true;

explain select /*+ STREAMTABLE(a) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;
select /*+ STREAMTABLE(a) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;

explain select /*+ STREAMTABLE(b) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;
select /*+ STREAMTABLE(b) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;

explain select /*+ STREAMTABLE(c) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;
select /*+ STREAMTABLE(c) */ * from T1 a join T2 b on a.key=b.key join T3 c on a.key=c.key;

explain select /*+ STREAMTABLE(a) */ a.key,b.val,c.* from T1 a join T1 b on a.key+10=b.val join T1 c on a.key+10=c.val;
select /*+ STREAMTABLE(a) */ a.key,b.val,c.* from T1 a join T1 b on a.key+10=b.val join T1 c on a.key+10=c.val;
