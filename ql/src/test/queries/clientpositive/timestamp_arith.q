select cast('2011-01-01 01:01:02.4567' as timestamp) - cast('1970-01-01 01:01:01.5678' as timestamp) from src limit 1;
select cast('2011-01-01 01:01:02.4567' as timestamp) + cast('1970-01-01 01:01:01.5678' as timestamp) from src limit 1;

select cast('2012-01-01 00:00:00' as timestamp) + (cast('2011-01-01 04:00:00.4567' as timestamp) - cast('2011-01-01 02:15:00.5678' as timestamp)) from src limit 1;

-- null (negative time is not supported)
select cast('2010-01-01 01:01:01.5678' as timestamp) - cast('2011-01-01 01:01:02.4567' as timestamp) from src limit 1;