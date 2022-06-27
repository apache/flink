-- SORT_QUERY_RESULTS

select * from src where src.key in (select c.key from (select * from src b where exists (select a.key from src a where b.value = a.value)) c);

[+I[1, val1], +I[2, val2], +I[3, val3]]

select * from src x where x.key in (select y.key from src y where exists (select z.key from src z where y.key = z.key));

[+I[1, val1], +I[2, val2], +I[3, val3]]

select * from src x join src y on x.key = y.key where exists (select * from src z where z.value = x.value and z.value = y.value);

[+I[1, val1, 1, val1], +I[2, val2, 2, val2], +I[3, val3, 3, val3]]

select * from (select x.key from src x);

[+I[1], +I[2], +I[3]]
