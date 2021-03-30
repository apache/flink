-- SORT_QUERY_RESULTS

select src.key,src.`[k].*` from src;

select * from bar where i in (1,2,3);

select * from bar where i between 1 and 3;

select value from src where key=_UTF-8 0xE982B5E993AE;

select (case when i>1 then 100 else split(s,',')[0] end) as a from bar;

select if(i>1,s,null) from bar;

select case when i>1 then array('1') else array(s) end from bar;

select stack(2,*) as (c1,c2) from srcpart;

select coalesce('abc',1);

select default.hiveudf(y) from foo;
