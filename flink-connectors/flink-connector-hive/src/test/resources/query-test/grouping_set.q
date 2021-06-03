-- SORT_QUERY_RESULTS

select x,y,grouping__id,sum(1) from foo group by x,y grouping sets ((x,y),(x));

[+I[1, 1, 0, 1], +I[1, null, 1, 1], +I[2, 2, 0, 1], +I[2, null, 1, 1], +I[3, 3, 0, 1], +I[3, null, 1, 1], +I[4, 4, 0, 1], +I[4, null, 1, 1], +I[5, 5, 0, 1], +I[5, null, 1, 1]]

select x,y,grouping(x),sum(1) from foo group by x,y grouping sets ((x,y),(x));

[+I[1, 1, 0, 1], +I[1, null, 0, 1], +I[2, 2, 0, 1], +I[2, null, 0, 1], +I[3, 3, 0, 1], +I[3, null, 0, 1], +I[4, 4, 0, 1], +I[4, null, 0, 1], +I[5, 5, 0, 1], +I[5, null, 0, 1]]
