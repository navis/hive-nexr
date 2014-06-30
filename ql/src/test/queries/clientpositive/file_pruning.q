-- SORT_QUERY_RESULTS

create temporary function pathname as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPathName';

set hive.optimize.ppd.vc.filename=true;
set hive.auto.convert.join=false;

-- HIVE-1662 File Prunning by filter on INPUT__FILE__NAME(VC)
-- srcbucket2 has 4 files (srcbucket20, srcbucket21, srcbucket22, srcbucket23)
-- and selects srcbucket20 and srcbucket23 as input

explain extended
select * from (select key, value, pathname(INPUT__FILE__NAME) as filename from srcbucket2) A
    where filename rlike 'srcbucket2[03].txt' AND key < 100 order by filename, key, value;

select * from (select key, value, pathname(INPUT__FILE__NAME) as filename from srcbucket2) A
    where filename rlike 'srcbucket2[03].txt' AND key < 100 order by filename, key, value;

drop temporary function pathname;
