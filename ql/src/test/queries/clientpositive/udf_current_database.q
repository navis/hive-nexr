DESCRIBE FUNCTION current_database;

explain
select current_database();
select current_database();

create database xxx;
use xxx;

explain
select current_database();
select current_database();
