explain select t.key from (select a.*, b.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;