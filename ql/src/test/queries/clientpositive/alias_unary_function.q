-- hive-2477
explain select key from (select cast(key as string) from src )t;