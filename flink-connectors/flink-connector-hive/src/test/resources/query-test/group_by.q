-- SORT_QUERY_RESULTS

select x,count(y),max(y) from foo group by x;

[+I[1, 1, 1], +I[2, 1, 2], +I[3, 1, 3], +I[4, 1, 4], +I[5, 1, 5]]

select count(distinct i) from bar group by s;

[+I[1], +I[1], +I[1]]

select max(c) from (select x,count(y) as c from foo group by x) t1;

[+I[1]]

select x,sum(y) as s from foo group by x having min(y)>1;

[+I[2, 2], +I[3, 3], +I[4, 4], +I[5, 5]]

select sum(x) as s1 from foo group by y having s1 > 2 and avg(x) < 4;

[+I[3]]

select sum(x) as s1,y as y1 from foo group by y having s1 > 2 and y1 < 4;

[+I[3, 3]]

select dep,count(1) from employee where salary<5000 and age>=38 and dep='Sales' group by dep;

[+I[Sales, 1]]

select x,null as n from foo group by x,'a',null;

[+I[1, null], +I[2, null], +I[3, null], +I[4, null], +I[5, null]]
