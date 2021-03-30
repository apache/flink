-- SORT_QUERY_RESULTS

select count(x) from foo union all select i from bar;

select x from foo union select i from bar;

select i from bar except select x from foo;

select x from foo intersect select i from bar;
