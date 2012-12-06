-- negative, sum of cluster size exceeds 99 percent
explain select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 40 PERCENT,
   b.key = 100 CLUSTER BY 40 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);
