explain select x from foo sort by x;

[+I[== Abstract Syntax Tree ==
LogicalDistribution(collation=[[0 ASC-nulls-first]], dist=[[]])
+- LogicalProject(x=[$0])
   +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Sort(orderBy=[x ASC])
+- TableSourceScan(table=[[test-catalog, default, foo, project=[x]]], fields=[x])

== Optimized Execution Plan ==
Sort(orderBy=[x ASC])
+- TableSourceScan(table=[[test-catalog, default, foo, project=[x]]], fields=[x])
]]

explain select x from foo cluster by x;

[+I[== Abstract Syntax Tree ==
LogicalDistribution(collation=[[0 ASC-nulls-first]], dist=[[0]])
+- LogicalProject(x=[$0])
   +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Sort(orderBy=[x ASC])
+- Exchange(distribution=[hash[x]])
   +- TableSourceScan(table=[[test-catalog, default, foo, project=[x]]], fields=[x])

== Optimized Execution Plan ==
Sort(orderBy=[x ASC])
+- Exchange(distribution=[hash[x]])
   +- TableSourceScan(table=[[test-catalog, default, foo, project=[x]]], fields=[x])
]]

explain select x,y from foo distribute by y sort by x desc;

[+I[== Abstract Syntax Tree ==
LogicalDistribution(collation=[[0 DESC-nulls-last]], dist=[[1]])
+- LogicalProject(x=[$0], y=[$1])
   +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Sort(orderBy=[x DESC])
+- Exchange(distribution=[hash[y]])
   +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])

== Optimized Execution Plan ==
Sort(orderBy=[x DESC])
+- Exchange(distribution=[hash[y]])
   +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])
]]

explain select x,y from foo distribute by abs(y);

[+I[== Abstract Syntax Tree ==
LogicalProject(x=[$0], y=[$1])
+- LogicalDistribution(collation=[[]], dist=[[2]])
   +- LogicalProject(x=[$0], y=[$1], (tok_function abs (tok_table_or_col y))=[abs($1)])
      +- LogicalProject(x=[$0], y=[$1])
         +- LogicalTableScan(table=[[test-catalog, default, foo]])

== Optimized Physical Plan ==
Calc(select=[x, y])
+- Exchange(distribution=[hash[(tok_function abs (tok_table_or_col y))]])
   +- Calc(select=[x, y, abs(y) AS (tok_function abs (tok_table_or_col y))])
      +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])

== Optimized Execution Plan ==
Calc(select=[x, y])
+- Exchange(distribution=[hash[(tok_function abs (tok_table_or_col y))]])
   +- Calc(select=[x, y, abs(y) AS (tok_function abs (tok_table_or_col y))])
      +- TableSourceScan(table=[[test-catalog, default, foo]], fields=[x, y])
]]
