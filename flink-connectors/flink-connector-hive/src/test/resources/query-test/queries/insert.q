insert into dest select 0,y from foo sort by y;

explain insert into dest(y,x) select x,y from foo cluster by x;
insert into dest(y,x) select x,y from foo cluster by x;

explain insert into dest(y) select y from foo sort by y limit 1;
insert into dest(y) select y from foo sort by y limit 1;

insert into destp select x,'0','00' from foo order by x limit 1;

insert overwrite table destp partition(p='0',q) select 1,`value` from src sort by value;

insert into dest select * from src;

insert overwrite table destp partition (p='-1',q='-1') if not exists select x from foo;

insert into destp partition(p='1',q) (x,q) select * from bar;

insert into destp partition(p='1',q) (q) select s from bar;

insert into destp partition(p,q) (p,x) select s,i from bar;

insert into destp partition (p,q) (q,x) values ('a',2);
