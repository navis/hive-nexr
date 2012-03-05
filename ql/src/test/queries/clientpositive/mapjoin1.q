set hive.mapjoin.cache.numrows=100;

SELECT  /*+ MAPJOIN(b) */ sum(a.key) as sum_a
	FROM srcpart a
	JOIN src b ON a.key = b.key where a.ds is not null;

-- const filter on outer join
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND true limit 10;

-- func filter on outer join
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND b.key * 10 < '1000' limit 10;
