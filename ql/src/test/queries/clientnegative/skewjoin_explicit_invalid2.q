-- negative, skew expression is not containing join condition
explain select count(*) from src a JOIN src b ON a.key+1=b.key SKEWED ON (
   a.key = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);
