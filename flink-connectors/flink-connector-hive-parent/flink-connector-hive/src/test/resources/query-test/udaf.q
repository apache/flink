-- SORT_QUERY_RESULTS

select sum(x) from foo;

[+I[15]]

select percentile_approx(x, 0.5) from foo;

[+I[2.5]]

select i, collect_list(array(s)) from bar group by i;
[+I[1, [[a], [aa]]], +I[2, [[b]]]]
