-- SORT_QUERY_RESULTS

select s from foo join bar on foo.x=bar.i and foo.y=bar.i group by s order by s;

[+I[a], +I[aa], +I[b]]

select * from foo join (select max(i) as m from bar) a on foo.y=a.m;

[+I[2, 2, 2]]

select * from foo left outer join bar on foo.y=bar.i;

[+I[1, 1, 1, a], +I[1, 1, 1, aa], +I[2, 2, 2, b], +I[3, 3, null, null], +I[4, 4, null, null], +I[5, 5, null, null]]

select * from foo right outer join bar on foo.y=bar.i;

[+I[1, 1, 1, a], +I[1, 1, 1, aa], +I[2, 2, 2, b]]

select * from foo full outer join bar on foo.y=bar.i;

[+I[1, 1, 1, a], +I[1, 1, 1, aa], +I[2, 2, 2, b], +I[3, 3, null, null], +I[4, 4, null, null], +I[5, 5, null, null]]

select * from foo left semi join bar on foo.y=bar.i;

[+I[1, 1], +I[2, 2]]

select count(1) from (select x from foo where x = 1) foo1 left semi join (select i from bar where i = 1) bar2 on 1 = 1;
[+I[1]]

select * from foo left semi join bar on (foo.x + bar.i > 4);
[+I[3, 3], +I[4, 4], +I[5, 5]]

select * from (select a.value, a.* from (select * from src) a join (select * from src) b on a.key = b.key) t;

[+I[val1, 1, val1], +I[val2, 2, val2], +I[val3, 3, val3]]

select f1.x,f1.y,f2.x,f2.y from (select * from foo order by x,y) f1 join (select * from foo order by x,y) f2;

[+I[1, 1, 1, 1], +I[1, 1, 2, 2], +I[1, 1, 3, 3], +I[1, 1, 4, 4], +I[1, 1, 5, 5], +I[2, 2, 1, 1], +I[2, 2, 2, 2], +I[2, 2, 3, 3], +I[2, 2, 4, 4], +I[2, 2, 5, 5], +I[3, 3, 1, 1], +I[3, 3, 2, 2], +I[3, 3, 3, 3], +I[3, 3, 4, 4], +I[3, 3, 5, 5], +I[4, 4, 1, 1], +I[4, 4, 2, 2], +I[4, 4, 3, 3], +I[4, 4, 4, 4], +I[4, 4, 5, 5], +I[5, 5, 1, 1], +I[5, 5, 2, 2], +I[5, 5, 3, 3], +I[5, 5, 4, 4], +I[5, 5, 5, 5]]

select foo.y, bar.I from bar join foo on hiveudf(foo.x) = bar.I where bar.I > 1;

[+I[2, 2]]
