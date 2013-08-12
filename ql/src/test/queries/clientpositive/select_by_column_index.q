-- SORT_QUERY_RESULTS

explain
select $0, $1 from src TABLESAMPLE(10 ROWS) order by $0;
select $0, $1 from src TABLESAMPLE(10 ROWS) order by $0;

explain
select $1, sum($0) from src TABLESAMPLE(10 ROWS) group by $1;
select $1, sum($0) from src TABLESAMPLE(10 ROWS) group by $1;

explain
select a.$0, a.$1, b.$1 from src a join src1 b on a.$0=b.$0;
select a.$0, a.$1, b.$1 from src a join src1 b on a.$0=b.$0;
