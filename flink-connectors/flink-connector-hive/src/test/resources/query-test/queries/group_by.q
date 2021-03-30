-- SORT_QUERY_RESULTS

select x,count(y),max(y) from foo group by x;

select count(distinct i) from bar group by s;

select max(c) from (select x,count(y) as c from foo group by x) t1;

select x,sum(y) as s from foo group by x having min(y)>1;

select sum(x) as s1 from foo group by y having s1 > 2 and avg(x) < 4;

select sum(x) as s1,y as y1 from foo group by y having s1 > 2 and y1 < 4;

select dep,count(1) from employee where salary<5000 and age>=38 and dep='Sales' group by dep;

select x,null as n from foo group by x,'a',null;
