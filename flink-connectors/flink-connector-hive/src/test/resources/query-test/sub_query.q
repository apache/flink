-- SORT_QUERY_RESULTS

select * from src where src.key in (select c.key from (select * from src b where exists (select a.key from src a where b.value = a.value)) c);

[+I[1, val1], +I[2, val2], +I[3, val3]]

select * from src x where x.key in (select y.key from src y where exists (select z.key from src z where y.key = z.key));

[+I[1, val1], +I[2, val2], +I[3, val3]]
