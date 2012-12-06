-- negative, skew expression should be a boolean type
explain select count(*) from src a JOIN src b ON a.key+1=b.key SKEWED ON (
   a.key + 100 CLUSTER BY 20 PERCENT);
