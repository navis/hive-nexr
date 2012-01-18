explain select key from src t group by t.key;

explain select t.key from src t group by t.key sort by t.key;

explain select src.value from src JOIN src1 ON src.key = src1.key GROUP BY src.value ORDER BY src.value;