select x from foo order by x desc limit 1;

select 'x' as key_new , split(value,',') as value_new from src ORDER BY key_new ASC, value_new[0] ASC limit 20;

select * from foo where cast(x as double)<=0 order by cast(x as double);
