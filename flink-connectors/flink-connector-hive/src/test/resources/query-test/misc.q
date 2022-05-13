-- SORT_QUERY_RESULTS

select src.key,src.`[k].*` from src;

[+I[1, 1], +I[2, 2], +I[3, 3]]

select * from bar where i in (1,2,3);

[+I[1, a], +I[1, aa], +I[2, b]]

select * from bar where i between 1 and 3;

[+I[1, a], +I[1, aa], +I[2, b]]

select value from src where key=_UTF-8 0xE982B5E993AE;

[]

select (case when i>1 then 100 else split(s,',')[0] end) as a from bar;

[+I[100], +I[a], +I[aa]]

select if(i>1,s,null) from bar;

[+I[b], +I[null], +I[null]]

select case when i>1 then array('1') else array(s) end from bar;

[+I[[1]], +I[[a]], +I[[aa]]]

select stack(2,*) as (c1,c2) from srcpart;

[]

select coalesce('abc',1);

[+I[abc]]

select default.hiveudf(y) from foo;

[+I[1], +I[2], +I[3], +I[4], +I[5]]
