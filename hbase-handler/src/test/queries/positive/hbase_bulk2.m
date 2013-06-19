DROP table hbase_export;

CREATE EXTERNAL TABLE hbase_export(rowkey STRING, col1 STRING, col2 STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseExportSerDe'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf2:value,:key,cf1:key")
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.hbase.HiveHFileExporter'
LOCATION '/tmp/export';

set mapred.reduce.tasks=4;
set hive.optimize.sampling.orderby=true;

INSERT OVERWRITE TABLE hbase_export
SELECT * from (SELECT hfile_kv(key,key,value,"cf2:value,:key,cf1:key") as (key,values) FROM src) A ORDER BY key,values;

dfs -count /tmp/export;
dfs -lsr /tmp/export;

-- QTestUtil masks something like this (validated by HFile.Writer itself)
--            3            8              40392 hdfs://localhost.localdomain:50049/tmp/export
-- drwxr-xr-x   - navis supergroup          0 2013-06-24 11:08 /tmp/export/cf1
-- -rw-r--r--   1 navis supergroup       4274 2013-06-24 11:08 /tmp/export/cf1/62839fa5a60d40aab210b4159fc94428
-- -rw-r--r--   1 navis supergroup       6324 2013-06-24 11:08 /tmp/export/cf1/961670eae78649caa777d1adce381802
-- -rw-r--r--   1 navis supergroup       4977 2013-06-24 11:08 /tmp/export/cf1/d618fd415c4a42d8b980e55ad0accdf1
-- -rw-r--r--   1 navis supergroup       4113 2013-06-24 11:08 /tmp/export/cf1/f90bbc7d0b3741689c0385692516c94d
-- drwxr-xr-x   - navis supergroup          0 2013-06-24 11:08 /tmp/export/cf2
-- -rw-r--r--   1 navis supergroup       4323 2013-06-24 11:08 /tmp/export/cf2/0b85ebc7f5974d81a2b321ac2a87569f
-- -rw-r--r--   1 navis supergroup       6732 2013-06-24 11:08 /tmp/export/cf2/38765a24c54c49e38a6033f4f2c951c1
-- -rw-r--r--   1 navis supergroup       5200 2013-06-24 11:08 /tmp/export/cf2/57a12a447137484b828165842c3da0f8
-- -rw-r--r--   1 navis supergroup       4449 2013-06-24 11:08 /tmp/export/cf2/c5471ba48d3d400e82f022c893e62c80

desc formatted hbase_export;
