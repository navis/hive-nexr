-- table
create table test_table (id int, query string, name string);
describe formatted test_table;

alter table test_table set retention 120 sec;
describe formatted test_table;

alter table test_table set retention 30 min;
describe formatted test_table;

alter table test_table set retention 12 hours;
describe formatted test_table;

alter table test_table set retention 7 days;
describe formatted test_table;

alter table test_table unset retention;
describe formatted test_table;

drop table test_table;

-- partitioned table
create table test_table (id int, query string, name string) partitioned by (ds string, hr string);
describe formatted test_table;

alter table test_table set retention 120 sec;
describe formatted test_table;

alter table test_table set retention 30 min;
describe formatted test_table;

alter table test_table set retention 12 hours;
describe formatted test_table;

alter table test_table set retention 7 days;
describe formatted test_table;

alter table test_table unset retention;
describe formatted test_table;

drop table test_table;
