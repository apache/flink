-- SORT_QUERY_RESULTS

select s from foo join bar on foo.x=bar.i and foo.y=bar.i group by s order by s;

select * from foo join (select max(i) as m from bar) a on foo.y=a.m;

select * from foo left outer join bar on foo.y=bar.i;

select * from foo right outer join bar on foo.y=bar.i;

select * from foo full outer join bar on foo.y=bar.i;

select * from foo left semi join bar on foo.y=bar.i;

select * from (select a.value, a.* from (select * from src) a join (select * from src) b on a.key = b.key) t;

select f1.x,f1.y,f2.x,f2.y from (select * from foo order by x,y) f1 join (select * from foo order by x,y) f2;
