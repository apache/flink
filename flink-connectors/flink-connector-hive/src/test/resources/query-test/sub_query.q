-- SORT_QUERY_RESULTS

select * from src where src.key in (select c.key from (select * from src b where exists (select a.key from src a where b.value = a.value)) c);

[+I[1, val1], +I[2, val2], +I[3, val3]]

select * from src x where x.key in (select y.key from src y where exists (select z.key from src z where y.key = z.key));

[+I[1, val1], +I[2, val2], +I[3, val3]]

select * from src x join src y on x.key = y.key where exists (select * from src z where z.value = x.value and z.value = y.value);

[+I[1, val1, 1, val1], +I[2, val2, 2, val2], +I[3, val3, 3, val3]]

select * from (select x.key from src x) as t;

[+I[1], +I[2], +I[3]]

SELECT * FROM bar WHERE i IN (SELECT x FROM t_sub_query);

[+I[2, b]]

SELECT i, count(s) FROM bar group by i having i NOT IN (SELECT x FROM t_sub_query);

[+I[1, 2]]

select * from foo where x IN (select count(*) from foo pp where pp.x = foo.x);

[+I[1, 1]]
