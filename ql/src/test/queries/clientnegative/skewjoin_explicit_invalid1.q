-- negative, constant expression
explain select count(*) from src a JOIN src b ON a.key=b.key SKEWED ON (
   0 = 0 CLUSTER BY 20 PERCENT,
   b.key = 100 CLUSTER BY 20 PERCENT,
   cast(a.key as int) > 300 CLUSTER BY 40 PERCENT);
