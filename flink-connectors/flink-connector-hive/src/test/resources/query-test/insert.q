insert into dest select 0,y from foo sort by y;

[+I[-1]]

explain insert into dest(y,x) select x,y from foo cluster by x;

[+I[== Abstract Syntax Tree ==
LogicalSink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- LogicalDistribution(collation=[[1 ASC-nulls-first]], dist=[[1]])
   +- LogicalProject($f0=[$1], $f1=[$0])
      +- LogicalProject(x=[$0], y=[$1])
         +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Sink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- Sort(orderBy=[$f1 ASC])
   +- Exchange(distribution=[hash[$f1]])
      +- Calc(select=[y AS $f0, x AS $f1])
         +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])

== Optimized Execution Plan ==
Sink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- Sort(orderBy=[$f1 ASC])
   +- Exchange(distribution=[hash[$f1]])
      +- Calc(select=[y AS $f0, x AS $f1])
         +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])
]]

insert into dest(y,x) select x,y from foo cluster by x;

[+I[-1]]

explain insert into dest(y) select y from foo sort by y limit 1;

[+I[== Abstract Syntax Tree ==
LogicalSink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- LogicalSort(offset=[0], fetch=[1])
   +- LogicalDistribution(collation=[[1 ASC-nulls-first]], dist=[[]])
      +- LogicalProject($f0=[null:INTEGER], $f1=[$0])
         +- LogicalProject(y=[$1])
            +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Sink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- Limit(offset=[0], fetch=[1], global=[true])
   +- Sort(orderBy=[$f1 ASC])
      +- Exchange(distribution=[single])
         +- Limit(offset=[0], fetch=[1], global=[false])
            +- Sort(orderBy=[$f1 ASC])
               +- Calc(select=[null:INTEGER AS $f0, y AS $f1])
                  +- TableSourceScan(table=[[test-catalog, default, foo, project=[y]]], fields=[y])

== Optimized Execution Plan ==
Sink(table=[test-catalog.default.dest], fields=[$f0, $f1])
+- Limit(offset=[0], fetch=[1], global=[true])
   +- Sort(orderBy=[$f1 ASC])
      +- Exchange(distribution=[single])
         +- Limit(offset=[0], fetch=[1], global=[false])
            +- Sort(orderBy=[$f1 ASC])
               +- Calc(select=[null:INTEGER AS $f0, y AS $f1])
                  +- TableSourceScan(table=[[test-catalog, default, foo, project=[y]]], fields=[y])
]]

insert into dest(y) select y from foo sort by y limit 1;

[+I[-1]]

insert into destp select x,'0','00' from foo order by x limit 1;

[+I[-1]]

insert overwrite table destp partition(p='0',q) select 1,`value` from src sort by value;

[+I[-1]]

insert into dest select * from src;

[+I[-1]]

insert overwrite table destp partition (p='-1',q='-1') if not exists select x from foo;

[+I[OK]]

insert into destp partition(p='1',q) (x,q) select * from bar;

[+I[-1]]

insert into destp partition(p='1',q) (q) select s from bar;

[+I[-1]]

insert into destp partition(p,q) (p,x) select s,i from bar;

[+I[-1]]

insert into destp partition (p,q) (q,x) values ('a',2);

[+I[-1]]