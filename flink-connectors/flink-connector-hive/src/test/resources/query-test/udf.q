-- SORT_QUERY_RESULTS

select bround(55.0, -1);

[+I[60]]

select bround(55.0, +1);

[+I[55]]

select round(123.45, -2);

[+I[100]]

select sha2('ABC', cast(null as int));

[+I[null]]

select x from foo where cast(unix_timestamp() as int) > 0;

[+I[1], +I[2], +I[3], +I[4], +I[5]]
