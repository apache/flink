-- SORT_QUERY_RESULTS

select x,y,grouping__id,sum(1) from foo group by x,y grouping sets ((x,y),(x));

select x,y,grouping(x),sum(1) from foo group by x,y grouping sets ((x,y),(x));
