set hive.input.percent.sampler=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat$DefaultPercentSampler;
select count(*) from srcbucket2 tablesample (1 percent);

set hive.input.percent.sampler=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat$HeadPercentSampler;
select count(*) from srcbucket2 tablesample (1 percent);
