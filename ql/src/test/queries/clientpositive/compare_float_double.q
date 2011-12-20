drop table emp;

CREATE TABLE emp (empno int, ename string, job string, mgr int, hiredate string, sal float, comm double, deptno int)
ROW FORMAT delimited fields terminated by ' ' STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/employee.txt' INTO TABLE emp;

select sal,comm from emp where sal < 1250.0f OR comm=0d;
select sal,comm from emp where sal >= 1250.0f AND sal < 1600.0f AND (comm IS NULL OR comm < 500.0d);
select sal,comm from emp where sal >= 1600.0f OR comm >= 500d;

drop table emp;