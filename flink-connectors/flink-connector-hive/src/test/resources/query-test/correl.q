-- SORT_QUERY_RESULTS

select * from foo where y in (select i from bar);

[+I[1, 1], +I[2, 2]]

select (select count(x) from foo where foo.y=bar.i) from bar;

[+I[1], +I[1], +I[1]]

select key, value from src where key in (select key+18 from src) order by key;

[]

select x from foo where x in (select key from src);

[+I[1], +I[2], +I[3]]
