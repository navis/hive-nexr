set X=someX;
set Y=someY;

explain
SELECT TRANSFORM('echo ${X:-x} ${Y:-y} ${Z:-z}') USING 'sh' AS x,y,z WITH ('Z'='someZ', 'Y')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
FROM src tablesample (1 rows);

SELECT TRANSFORM('echo ${X:-x} ${Y:-y} ${Z:-z}') USING 'sh' AS x,y,z WITH ('Z'='someZ', 'Y')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
FROM src tablesample (1 rows);
