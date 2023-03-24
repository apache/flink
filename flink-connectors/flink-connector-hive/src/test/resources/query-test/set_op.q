-- SORT_QUERY_RESULTS

select count(x) from foo union all select i from bar;

[+I[1], +I[1], +I[2], +I[5]]

select x from foo union select i from bar;

[+I[1], +I[2], +I[3], +I[4], +I[5]]

select i from bar except select x from foo;

[]

select x from foo intersect select i from bar;

[+I[1], +I[2]]

select x,y from foo union all select i,i from bar;

[+I[1, 1], +I[1, 1], +I[1, 1], +I[2, 2], +I[2, 2], +I[3, 3], +I[4, 4], +I[5, 5]]

select x,Y,X from foo union all select i,i,I from bar;

[+I[1, 1, 1], +I[1, 1, 1], +I[1, 1, 1], +I[2, 2, 2], +I[2, 2, 2], +I[3, 3, 3], +I[4, 4, 4], +I[5, 5, 5]]
