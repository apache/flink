---
title: "查询语句"
nav-parent_id: sql
nav-pos: 1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}
<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="java/scala" markdown="1">

SELECT 语句和 VALUES 语句需要使用 `TableEnvironment` 的 `sqlQuery()` 方法加以指定。这个方法会以 `Table` 的形式返回 SELECT （或 VALUE）的查询结果。`Table` 可以被用于 [随后的SQL 与 Table API 查询]({{ site.baseurl }}/zh/dev/table/common.html#mixing-table-api-and-sql) 、 [转换为 DataSet 或 DataStream ]({{ site.baseurl }}/zh/dev/table/common.html#integration-with-datastream-and-dataset-api)或 [输出到 TableSink ]({{ site.baseurl }}/zh/dev/table/common.html#emit-a-table)。SQL 与 Table API 的查询可以进行无缝融合、整体优化并翻译为单一的程序。

为了可以在 SQL 查询中访问到表，你需要先 [在 TableEnvironment 中注册表 ]({{ site.baseurl }}/zh/dev/table/common.html#register-tables-in-the-catalog)。表可以通过 [TableSource]({{ site.baseurl }}/zh/dev/table/common.html#register-a-tablesource)、 [Table]({{ site.baseurl }}/zh/dev/table/common.html#register-a-table)、[CREATE TABLE 语句](create.html)、 [DataStream 或 DataSet]({{ site.baseurl }}/zh/dev/table/common.html#register-a-datastream-or-dataset-as-table) 注册。 用户也可以通过 [向 TableEnvironment 中注册 catalog ]({{ site.baseurl }}/zh/dev/table/catalogs.html) 的方式指定数据源的位置。

为方便起见 `Table.toString()` 将会在其 `TableEnvironment` 中自动使用一个唯一的名字注册表并返回表名。 因此， `Table` 对象可以如下文所示样例，直接内联到 SQL 语句中。

**注意：** 查询若包括了不支持的 SQL 特性，将会抛出 `TableException`。批处理和流处理所支持的 SQL 特性将会在下述章节中列出。

</div>

<div data-lang="python" markdown="1">

SELECT 语句和 VALUES 语句需要使用 `TableEnvironment` 的 `sqlQuery()` 方法加以指定。这个方法会以 `Table` 的形式返回 SELECT （或 VALUE）的查询结果。`Table` 可以被用于 [随后的SQL 与 Table API 查询]({{ site.baseurl }}/zh/dev/table/common.html#mixing-table-api-and-sql) 或 [输出到 TableSink ]({{ site.baseurl }}/zh/dev/table/common.html#emit-a-table)。SQL 与 Table API 的查询可以进行无缝融合、整体优化并翻译为单一的程序。

为了可以在 SQL 查询中访问到表，你需要先 [在 TableEnvironment 中注册表 ]({{ site.baseurl }}/zh/dev/table/common.html#register-tables-in-the-catalog)。表可以通过 [TableSource]({{ site.baseurl }}/zh/dev/table/common.html#register-a-tablesource)、 [Table]({{ site.baseurl }}/zh/dev/table/common.html#register-a-table)、[CREATE TABLE 语句](create.html) 注册。 用户也可以通过 [向 TableEnvironment 中注册 catalog ]({{ site.baseurl }}/zh/dev/table/catalogs.html) 的方式指定数据源的位置。

为方便起见 `str(Table)` 将会在其 `TableEnvironment` 中自动使用一个唯一的名字注册表并返回表名。 因此， `Table` 对象可以如下文所示样例，直接内联到 SQL 语句中。

**注意：** 查询若包括了不支持的 SQL 特性，将会抛出 `TableException`。批处理和流处理所支持的 SQL 特性将会在下述章节中列出。

</div>
</div>

## 指定查询

以下示例显示如何在已注册和内联表上指定 SQL 查询。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 从外部数据源读取 DataStream 
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);

// 使用 SQL 查询内联的（未注册的）表
Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
Table result = tableEnv.sqlQuery(
  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

// SQL 查询一个已经注册的表
// 根据视图 "Orders" 创建一个 DataStream
tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"));
// 在表上执行 SQL 查询并得到以新表返回的结果
Table result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// 创建并注册一个 TableSink
final Schema schema = new Schema()
    .field("product", DataTypes.STRING())
    .field("amount", DataTypes.INT());

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(...)
    .withSchema(schema)
    .createTemporaryTable("RubberOrders");

// 在表上执行插入语句并把结果发出到 TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

//  从外部数据源读取 DataStream 
val ds: DataStream[(Long, String, Integer)] = env.addSource(...)

// 使用 SQL 查询内联的（未注册的）表
val table = ds.toTable(tableEnv, $"user", $"product", $"amount")
val result = tableEnv.sqlQuery(
  s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

// 使用名称 "Orders" 注册一个 DataStream 
tableEnv.createTemporaryView("Orders", ds, $"user", $"product", $"amount")
// 在表上执行 SQL 查询并得到以新表返回的结果
val result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")

// 创建并注册一个 TableSink
val schema = new Schema()
    .field("product", DataTypes.STRING())
    .field("amount", DataTypes.INT())

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(...)
    .withSchema(schema)
    .createTemporaryTable("RubberOrders")

// 在表上执行插入操作，并把结果发出到 TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# SQL 查询内联的（未注册的）表
# 元素数据类型： BIGINT, STRING, BIGINT
table = table_env.from_elements(..., ['user', 'product', 'amount'])
result = table_env \
    .sql_query("SELECT SUM(amount) FROM %s WHERE product LIKE '%%Rubber%%'" % table)

# 创建并注册 TableSink
t_env.connect(FileSystem().path("/path/to/file")))
    .with_format(Csv()
                 .field_delimiter(',')
                 .deriveSchema())
    .with_schema(Schema()
                 .field("product", DataTypes.STRING())
                 .field("amount", DataTypes.BIGINT()))
    .create_temporary_table("RubberOrders")

# 在表上执行插入操作，并把结果发出到 TableSink
table_env \
    .execute_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

## 执行查询

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="java/scala" markdown="1">

SELECT 语句或者 VALUES 语句可以通过 `TableEnvironment.executeSql()` 方法来执行，将选择的结果收集到本地。该方法返回 `TableResult` 对象用于包装查询的结果。和 SELECT 语句很像，一个 `Table` 对象可以通过 `Table.execute()` 方法执行从而将 `Table` 的内容收集到本地客户端。
`TableResult.collect()` 方法返回一个可以关闭的行迭代器。除非所有的数据都被收集到本地，否则一个查询作业永远不会结束。所以我们应该通过 `CloseableIterator#close()` 方法主动地关闭作业以防止资源泄露。
我们还可以通过 `TableResult.print()` 方法将查询结果打印到本地控制台。`TableResult` 中的结果数据只能被访问一次，因此一个 `TableResult` 实例中，`collect()` 方法和 `print()` 方法不能被同时使用。

`TableResult.collect()` 与 `TableResult.print()` 的行为在不同的 checkpointing 模式下略有不同（流作业开启 checkpointing 的方法可参考 <a href="{{ site.baseurl }}/zh/ops/config.html#checkpointing">checkpointing 配置</a>）。
* 对于批作业或没有配置任何 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 既不保证精确一次的数据交付、也不保证至少一次的数据交付。查询结果在产生后可被客户端即刻访问，但作业失败并重启时将会报错。
* 对于配置了精准一次 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 保证端到端精确一次的数据交付。一条结果数据只有在其对应的 checkpointing 完成后才能在客户端被访问。
* 对于配置了至少一次 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 保证端到端至少一次的数据交付。查询结果在产生后可被客户端即刻访问，但同一条结果可能被多次传递给客户端。

</div>

<div data-lang="python" markdown="1">

SELECT 语句或者 VALUES 语句可以通过 `TableEnvironment.execute_sql()` 方法来执行，将选择的结果收集到本地。该方法返回 `TableResult` 对象用于包装查询的结果。和 SELECT 语句很像，一个 `Table` 对象可以通过 `Table.execute()` 方法执行从而将 `Table` 的内容收集到本地客户端。
`TableResult.collect()` 方法返回一个可以关闭的行迭代器。除非所有的数据都被收集到本地，否则一个查询作业永远不会结束。所以我们应该通过 `CloseableIterator#close()` 方法主动地关闭作业以防止资源泄露。
我们还可以通过 `TableResult.print()` 方法将查询结果打印到本地控制台。`TableResult` 中的结果数据只能被访问一次，因此一个 `TableResult` 实例中，`collect()` 方法和 `print()` 方法不能被同时使用。

`TableResult.collect()` 与 `TableResult.print()` 的行为在不同的 checkpointing 模式下略有不同（流作业开启 checkpointing 的方法可参考 <a href="{{ site.baseurl }}/zh/ops/config.html#checkpointing">checkpointing 配置</a>）。
* 对于批作业或没有配置任何 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 既不保证精确一次的数据交付、也不保证至少一次的数据交付。查询结果在产生后可被客户端即刻访问，但作业失败并重启时将会报错。
* 对于配置了精准一次 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 保证端到端精确一次的数据交付。一条结果数据只有在其对应的 checkpointing 完成后才能在客户端被访问。
* 对于配置了至少一次 checkpointing 的流作业，`TableResult.collect()` 与 `TableResult.print()` 保证端到端至少一次的数据交付。查询结果在产生后可被客户端即刻访问，但同一条结果可能被多次传递给客户端。

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// execute SELECT statement
TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM Orders");
// use try-with-resources statement to make sure the iterator will be closed automatically
try (CloseableIterator<Row> it = tableResult1.collect()) {
    while(it.hasNext()) {
        Row row = it.next();
        // handle row
    }
}

// execute Table
TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute();
tableResult2.print();

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tableEnv = StreamTableEnvironment.create(env, settings)
// enable checkpointing
tableEnv.getConfig.getConfiguration.set(
  ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
tableEnv.getConfig.getConfiguration.set(
  ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))

tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// execute SELECT statement
val tableResult1 = tableEnv.executeSql("SELECT * FROM Orders")
val it = tableResult1.collect()
try while (it.hasNext) {
  val row = it.next
  // handle row
}
finally it.close() // close the iterator to avoid resource leak

// execute Table
val tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute()
tableResult2.print()

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, settings)
# enable checkpointing
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")

table_env.execute_sql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

# execute SELECT statement
table_result1 = table_env.execute_sql("SELECT * FROM Orders")
table_result1.print()

# execute Table
table_result2 = table_env.sql_query("SELECT * FROM Orders").execute()
table_result2.print()

{% endhighlight %}
</div>
</div>

{% top %}

## 语法

Flink 通过支持标准 ANSI SQL的 [Apache Calcite](https://calcite.apache.org/docs/reference.html) 解析 SQL。

以下 BNF-语法 描述了批处理和流处理查询中所支持的 SQL 特性的超集。其中 [操作符](#操作符) 章节展示了所支持的特性的样例，并指明了哪些特性仅适用于批处理或流处理。

{% highlight sql %}
query:
  values
  | {
      select
      | selectWithoutFrom
      | query UNION [ ALL ] query
      | query EXCEPT query
      | query INTERSECT query
    }
    [ ORDER BY orderItem [, orderItem ]* ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start { ROW | ROWS } ]
    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY]

orderItem:
  expression [ ASC | DESC ]

select:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }
  FROM tableExpression
  [ WHERE booleanExpression ]
  [ GROUP BY { groupItem [, groupItem ]* } ]
  [ HAVING booleanExpression ]
  [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

selectWithoutFrom:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }

projectItem:
  expression [ [ AS ] columnAlias ]
  | tableAlias . *

tableExpression:
  tableReference [, tableReference ]*
  | tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]

joinCondition:
  ON booleanExpression
  | USING '(' column [, column ]* ')'

tableReference:
  tablePrimary
  [ matchRecognize ]
  [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
  [ TABLE ] [ [ catalogName . ] schemaName . ] tableName [ dynamicTableOptions ]
  | LATERAL TABLE '(' functionName '(' expression [, expression ]* ')' ')'
  | UNNEST '(' expression ')'

dynamicTableOptions:
  /*+ OPTIONS(key=val [, key=val]*) */

key:
  stringLiteral

val:
  stringLiteral

values:
  VALUES expression [, expression ]*

groupItem:
  expression
  | '(' ')'
  | '(' expression [, expression ]* ')'
  | CUBE '(' expression [, expression ]* ')'
  | ROLLUP '(' expression [, expression ]* ')'
  | GROUPING SETS '(' groupItem [, groupItem ]* ')'

windowRef:
    windowName
  | windowSpec

windowSpec:
    [ windowName ]
    '('
    [ ORDER BY orderItem [, orderItem ]* ]
    [ PARTITION BY expression [, expression ]* ]
    [
        RANGE numericOrIntervalExpression {PRECEDING}
      | ROWS numericExpression {PRECEDING}
    ]
    ')'

matchRecognize:
      MATCH_RECOGNIZE '('
      [ PARTITION BY expression [, expression ]* ]
      [ ORDER BY orderItem [, orderItem ]* ]
      [ MEASURES measureColumn [, measureColumn ]* ]
      [ ONE ROW PER MATCH ]
      [ AFTER MATCH
            ( SKIP TO NEXT ROW
            | SKIP PAST LAST ROW
            | SKIP TO FIRST variable
            | SKIP TO LAST variable
            | SKIP TO variable )
      ]
      PATTERN '(' pattern ')'
      [ WITHIN intervalLiteral ]
      DEFINE variable AS condition [, variable AS condition ]*
      ')'

measureColumn:
      expression AS alias

pattern:
      patternTerm [ '|' patternTerm ]*

patternTerm:
      patternFactor [ patternFactor ]*

patternFactor:
      variable [ patternQuantifier ]

patternQuantifier:
      '*'
  |   '*?'
  |   '+'
  |   '+?'
  |   '?'
  |   '??'
  |   '{' { [ minRepeat ], [ maxRepeat ] } '}' ['?']
  |   '{' repeat '}'

{% endhighlight %}

Flink SQL 对于标识符（表、属性、函数名）有类似于 Java 的词法约定:

- 不管是否引用标识符，都保留标识符的大小写。
- 且标识符需区分大小写。
- 与 Java 不一样的地方在于，通过反引号，可以允许标识符带有非字母的字符（如：<code>"SELECT a AS `my field` FROM t"</code>）。

字符串文本常量需要被单引号包起来（如 `SELECT 'Hello World'` ）。两个单引号表示转移（如 `SELECT 'It''s me.'`）。字符串文本常量支持 Unicode 字符，如需明确使用 Unicode 编码，请使用以下语法：

- 使用反斜杠（`\`）作为转义字符（默认）：`SELECT U&'\263A'`
- 使用自定义的转义字符： `SELECT U&'#263A' UESCAPE '#'`

{% top %}

## 操作符

### Scan、Projection 与 Filter

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Scan / Select / As</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT * FROM Orders

SELECT a, c AS d FROM Orders
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Where / Filter</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT * FROM Orders WHERE b = 'red'

SELECT * FROM Orders WHERE a % 2 = 0
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>用户定义标量函数（Scalar UDF）</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
      <p>自定义函数必须事先注册到 TableEnvironment 中。 可阅读 <a href="{{ site.baseurl }}/zh/dev/table/functions/udfs.html">自定义函数文档</a> 以获得如何指定和注册自定义函数的详细信息。</p>
{% highlight sql %}
SELECT PRETTY_PRINT(user) FROM Orders
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### 聚合

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>GroupBy 聚合</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span><br>
        <span class="label label-info">结果更新</span>
      </td>
      <td>
        <p><b>注意：</b> GroupBy 在流处理表中会产生更新结果（updating result）。详情请阅读 <a href="{{ site.baseurl }}/zh/dev/table/streaming/dynamic_tables.html">动态表流概念</a> 。
        </p>
{% highlight sql %}
SELECT a, SUM(b) as d
FROM Orders
GROUP BY a
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>GroupBy 窗口聚合</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>使用分组窗口对每个组进行计算并得到一个结果行。详情请阅读 <a href="#分组窗口">分组窗口</a> 章节</p>
{% highlight sql %}
SELECT user, SUM(amount)
FROM Orders
GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Over Window aggregation</strong><br>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p><b>注意：</b> 所有的聚合必须定义到同一个窗口中，即相同的分区、排序和区间。当前仅支持 PRECEDING (无界或有界) 到 CURRENT ROW 范围内的窗口、FOLLOWING 所描述的区间并未支持，ORDER BY 必须指定于单个的<a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html">时间属性</a>。</p>
{% highlight sql %}
SELECT COUNT(amount) OVER (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM Orders

SELECT COUNT(amount) OVER w, SUM(amount) OVER w
FROM Orders 
WINDOW w AS (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span> <br>
        <span class="label label-info">结果更新</span>
      </td>
      <td>
{% highlight sql %}
SELECT DISTINCT users FROM Orders
{% endhighlight %}
       <p><b>注意：</b> 对于流处理查询，根据不同字段的数量，计算查询结果所需的状态可能会无限增长。请提供具有有效保留间隔的查询配置，以防止出现过多的状态。请阅读 <a href="{{ site.baseurl }}/zh/dev/table/streaming/query_configuration.html">查询配置</a> 以获取详细的信息</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Grouping sets, Rollup, Cube</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
        <span class="label label-info">结果更新</span>
      </td>
      <td>
{% highlight sql %}
SELECT SUM(amount)
FROM Orders
GROUP BY GROUPING SETS ((user), (product))
{% endhighlight %}
        <p><b>Note:</b> 流式 Grouping sets、Rollup 以及 Cube 只在 Blink planner 中支持。</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Having</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT SUM(amount)
FROM Orders
GROUP BY users
HAVING SUM(amount) > 50
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>用户自定义聚合函数 (UDAGG)</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>UDAGG 必须注册到 TableEnvironment. 参考<a href="{{ site.baseurl }}/zh/dev/table/functions/udfs.html">自定义函数文档</a> 以了解如何指定和注册 UDAGG 。</p>
{% highlight sql %}
SELECT MyAggregate(amount)
FROM Orders
GROUP BY users
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### Joins

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Inner Equi-join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>目前仅支持 equi-join ，即 join 的联合条件至少拥有一个相等谓词。不支持任何 cross join 和 theta join。</p>
        <p><b>注意：</b> Join 的顺序没有进行优化，join 会按照 FROM 中所定义的顺序依次执行。请确保 join 所指定的表在顺序执行中不会产生不支持的 cross join （笛卡儿积）以至查询失败。</p>
{% highlight sql %}
SELECT *
FROM Orders INNER JOIN Product ON Orders.productId = Product.id
{% endhighlight %}
        <p><b>注意：</b> 流查询中可能会因为不同行的输入数量导致计算结果的状态无限增长。请提供具有有效保留间隔的查询配置，以防止出现过多的状态。详情请参考 <a href="{{ site.baseurl }}/zh/dev/table/streaming/query_configuration.html">查询配置</a> 页面.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Outer Equi-join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
        <span class="label label-info">结果更新</span>
      </td>
      <td>
        <p>目前仅支持 equi-join ，即 join 的联合条件至少拥有一个相等谓词。不支持任何 cross join 和 theta join。</p>
        <p><b>注意：</b> Join 的顺序没有进行优化，join 会按照 FROM 中所定义的顺序依次执行。请确保 join 所指定的表在顺序执行中不会产生不支持的 cross join （笛卡儿积）以至查询失败。</p>
{% highlight sql %}
SELECT *
FROM Orders LEFT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders RIGHT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders FULL OUTER JOIN Product ON Orders.productId = Product.id
{% endhighlight %}
        <p><b>注意：</b> 流查询中可能会因为不同行的输入数量导致计算结果的状态无限增长。请提供具有有效保留间隔的查询配置，以防止出现过多的状态。详情请参考 <a href="{{ site.baseurl }}/zh/dev/table/streaming/query_configuration.html">查询配置</a> 页面.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Interval Join</strong><br>
        <span class="label label-primary">批处理</span>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p><b>注意：</b>Interval join （时间区间关联）是常规 join 的子集，可以使用流的方式进行处理。</p>

        <p>Interval join需要至少一个 equi-join 谓词和一个限制了双方时间的 join 条件。例如使用两个适当的范围谓词（<code>&lt;, &lt;=, &gt;=, &gt;</code>），一个 <code>BETWEEN</code> 谓词或一个比较两个输入表中相同类型的 <a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html">时间属性</a> （即处理时间和事件时间）的相等谓词</p>
        <p>比如，以下谓词是合法的 interval join 条件：</p>

        <ul>
          <li><code>ltime = rtime</code></li>
          <li><code>ltime &gt;= rtime AND ltime &lt; rtime + INTERVAL '10' MINUTE</code></li>
          <li><code>ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND</code></li>
        </ul>

{% highlight sql %}
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

以上示例中，所有在收到后四小时内发货的 order 会与他们相关的 shipment 进行 join。
      </td>
    </tr>
    <tr>
      <td>
        <strong>Expanding arrays into a relation</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>目前尚未支持非嵌套的 WITH ORDINALITY 。</p>
{% highlight sql %}
SELECT users, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join 表函数 (UDTF)</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>将表与表函数的结果进行 join 操作。左表（outer）中的每一行将会与调用表函数所产生的所有结果中相关联行进行 join 。</p>
        <p>用户自定义表函数（ User-defined table functions，UDTFs ） 在执行前必须先注册。请参考 <a href="{{ site.baseurl }}/zh/dev/table/functions/udfs.html">UDF 文档</a> 以获取更多关于指定和注册UDF的信息 </p>

        <p><b>Inner Join</b></p>
        <p>若表函数返回了空结果，左表（outer）的行将会被删除。</p>
{% highlight sql %}
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags)) AS t(tag)
-- 从1.11开始，也可以使用下面的方式：
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags)) AS t(tag)
{% endhighlight %}

        <p><b>Left Outer Join</b></p>
        <p>若表函数返回了空结果，将会保留相对应的外部行并用空值填充结果。</p>
{% highlight sql %}
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) AS t(tag) ON TRUE
-- 从1.11开始，也可以使用下面的方式：
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) AS t(tag) ON TRUE
{% endhighlight %}

        <p><b>注意：</b> 当前仅支持文本常量 <code>TRUE</code> 作为针对横向表的左外部联接的谓词。</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join Temporal Table Function</strong><br>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p><a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html">Temporal Tables</a> 是跟随时间变化而变化的表。</p>
        <p><a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table-functions"> Temporal Table Function</a> 提供访问 Temporal Tables 在某一时间点的状态的能力。
        Join Temporal Table Function 的语法与 <i>Join Table Function</i> 一致。</p>

        <p><b>注意：</b> 目前仅支持在 Temporal Tables 上的 inner join 。</p>

        <p>假如 <i>Rates</i> 是一个 <a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table-functions"> Temporal Table Function</a>, join 可以使用 SQL 进行如下的表达:</p>
{% highlight sql %}
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
{% endhighlight %}
        <p>请查看 <a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html"> Temporal Tables 概念描述</a> 以了解详细信息。</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join Temporal Tables </strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p><a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html">Temporal Tables</a> 是随时间变化而变化的表。
        <a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table">Temporal Table</a> 提供访问指定时间点的 temporal table 版本的功能。</p>

        <p>仅支持带有处理时间的 temporal tables 的 inner 和 left join。</p>
        <p>下述示例中，假设 <strong>LatestRates</strong> 是一个根据最新的 rates 物化的 <a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table">Temporal Table</a> 。</p>
{% highlight sql %}
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
{% endhighlight %}
        <p>请阅读 <a href="{{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html">Temporal Tables</a> 概念描述以了解详细信息。</p>
        <p>仅 Blink planner 支持。</p>
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

### 集合操作

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Union</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>UnionAll</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION ALL
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Intersect / Except</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  INTERSECT
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  EXCEPT
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>In</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>若表达式在给定的表子查询中存在，则返回 true 。子查询表必须由单个列构成，且该列的数据类型需与表达式保持一致。</p>
{% highlight sql %}
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
{% endhighlight %}
        <p><b>注意：</b> 在流查询中，这一操作将会被重写为 join 和 group 操作。该查询所需要的状态可能会由于不同的输入行数而导致无限增长。请在查询配置中提合理的保留间隔以避免产生状态过大。请阅读 <a href="{{ site.baseurl }}/zh/dev/table/streaming/query_configuration.html">查询配置</a> 以了解详细信息</p>
      </td>
    </tr>

    <tr>
      <td>
        <strong>Exists</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>若子查询的结果多于一行，将返回 true 。仅支持可以被通过 join 和 group 重写的操作。</p>
{% highlight sql %}
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
{% endhighlight %}
        <p><b>注意：</b> 在流查询中，这一操作将会被重写为 join 和 group 操作。该查询所需要的状态可能会由于不同的输入行数而导致无限增长。请在查询配置中提合理的保留间隔以避免产生状态过大。请阅读 <a href="{{ site.baseurl }}/zh/dev/table/streaming/query_configuration.html">查询配置</a> 以了解详细信息</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### OrderBy & Limit

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Order By</strong><br>
        <span class="label label-primary">批处理</span> <span class="label label-primary">流处理</span>
      </td>
      <td>
<b>注意：</b> 流处理结果需主要根据 <a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html">时间属性</a> 按照升序进行排序。支持使用其他排序属性。  

{% highlight sql %}
SELECT *
FROM Orders
ORDER BY orderTime
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Limit</strong><br>
        <span class="label label-primary">批处理</span>
      </td>
      <td>
<b>注意：</b> LIMIT 查询需要有一个 ORDER BY 字句。
{% highlight sql %}
SELECT *
FROM Orders
ORDER BY orderTime
LIMIT 3
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

### Top-N

<span class="label label-danger">注意</span> 目前仅 Blink 计划器支持 Top-N 。

Top-N 查询是根据列排序找到N个最大或最小的值。最大值集和最小值集都被视为是一种 Top-N 的查询。若在批处理或流处理的表中需要显示出满足条件的 N 个最底层记录或最顶层记录， Top-N 查询将会十分有用。得到的结果集将可以进行进一步的分析。

Flink 使用 OVER 窗口条件和过滤条件相结合以进行 Top-N 查询。利用 OVER 窗口的 `PARTITION BY` 子句的功能，Flink 还支持逐组 Top-N 。 例如，每个类别中实时销量最高的前五种产品。批处理表和流处理表都支持基于SQL的 Top-N 查询。
以下是 TOP-N 表达式的语法:

{% highlight sql %}
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]
{% endhighlight %}

**参数说明：**

- `ROW_NUMBER()`: 根据当前分区内的各行的顺序从第一行开始，依次为每一行分配一个唯一且连续的号码。目前，我们只支持 `ROW_NUMBER` 在 over 窗口函数中使用。未来将会支持 `RANK()` 和 `DENSE_RANK()`函数。
- `PARTITION BY col1[, col2...]`: 指定分区列，每个分区都将会有一个 Top-N 结果。
- `ORDER BY col1 [asc|desc][, col2 [asc|desc]...]`: 指定排序列，不同列的排序方向可以不一样。
- `WHERE rownum <= N`: Flink 需要 `rownum <= N` 才能识别一个查询是否为 Top-N 查询。 其中， N 代表最大或最小的 N 条记录会被保留。
- `[AND conditions]`: 在 where 语句中，可以随意添加其他的查询条件，但其他条件只允许通过 `AND` 与 `rownum <= N` 结合使用。

<span class="label label-danger">流处理模式需注意</span> TopN 查询的结果会带有更新。 Flink SQL 会根据排序键对输入的流进行排序；若 top N 的记录发生了变化，变化的部分会以撤销、更新记录的形式发送到下游。
推荐使用一个支持更新的存储作为 Top-N 查询的 sink 。另外，若 top N 记录需要存储到外部存储，则结果表需要拥有相同与 Top-N 查询相同的唯一键。

Top-N 的唯一键是分区列和 rownum 列的结合，另外 Top-N 查询也可以获得上游的唯一键。以下面的任务为例，`product_id` 是 `ShopSales` 的唯一键，然后 Top-N 的唯一键是 [`category`, `rownum`] 和 [`product_id`] 。

下面的样例描述了如何指定带有 Top-N 的 SQL 查询。这个例子的作用是我们上面提到的“查询每个分类实时销量最大的五个产品”。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// 接收来自外部数据源的 DataStream
DataStream<Tuple4<String, String, String, Long>> ds = env.addSource(...);
// 把 DataStream 注册为表，表名是 “ShopSales”
tableEnv.createTemporaryView("ShopSales", ds, "product_id, category, product_name, sales");

// 选择每个分类中销量前5的产品
Table result1 = tableEnv.sqlQuery(
  "SELECT * " +
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
  "   FROM ShopSales)" +
  "WHERE row_num <= 5");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// 读取外部数据源的 DataStream
val ds: DataStream[(String, String, String, Long)] = env.addSource(...)
// 注册名为 “ShopSales” 的 DataStream
tableEnv.createTemporaryView("ShopSales", ds, $"product_id", $"category", $"product_name", $"sales")


// 选择每个分类中销量前5的产品
val result1 = tableEnv.sqlQuery(
    """
      |SELECT *
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num
      |   FROM ShopSales)
      |WHERE row_num <= 5
    """.stripMargin)
{% endhighlight %}
</div>
</div>

#### 无排名输出优化

如上文所描述，`rownum` 字段会作为唯一键的其中一个字段写到结果表里面，这会导致大量的结果写出到结果表。比如，当原始结果（名为 `product-1001` ）从排序第九变化为排序第一时，排名 1-9 的所有结果都会以更新消息的形式发送到结果表。若结果表收到太多的数据，将会成为 SQL 任务的瓶颈。

优化方法是在 Top-N 查询的外部 SELECT 子句中省略 rownum 字段。由于前N条记录的数量通常不大，因此消费者可以自己对记录进行快速排序，因此这是合理的。去掉 rownum 字段后，上述的例子中，只有变化了的记录（ `product-1001` ）需要发送到下游，从而可以节省大量的对结果表的 IO 操作。

以下的例子描述了如何以这种方式优化上述的 Top-N 查询：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// 从外部数据源读取 DataStream
DataStream<Tuple4<String, String, String, Long>> ds = env.addSource(...);
// 把 DataStream 注册为表，表名是 “ShopSales”
tableEnv.createTemporaryView("ShopSales", ds, $("product_id"), $("category"), $("product_name"), $("sales"));

// 选择每个分类中销量前5的产品
Table result1 = tableEnv.sqlQuery(
  "SELECT product_id, category, product_name, sales " + // omit row_num field in the output
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
  "   FROM ShopSales)" +
  "WHERE row_num <= 5");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// 从外部数据源读取 DataStream
val ds: DataStream[(String, String, String, Long)] = env.addSource(...)
// 注册名为 “ShopSales” 的数据源
tableEnv.createTemporaryView("ShopSales", ds, $"product_id", $"category", $"product_name", $"sales")


// 选择每个分类中销量前5的产品
val result1 = tableEnv.sqlQuery(
    """
      |SELECT product_id, category, product_name, sales  -- omit row_num field in the output
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num
      |   FROM ShopSales)
      |WHERE row_num <= 5
    """.stripMargin)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">使用流处理模式时需注意</span> 为了使上述查询输出可以输出到外部存储并且结果正确，外部存储需要拥有与 Top-N 查询一致的唯一键。在上述的查询例子中，若 `product_id` 是查询的唯一键，那么外部表必须要有 `product_id` 作为其唯一键。

### 去重

<span class="label label-danger">注意</span> 仅 Blink planner 支持去重。

去重是指对在列的集合内重复的行进行删除，只保留第一行或最后一行数据。 在某些情况下，上游的 ETL 作业不能实现精确一次的端到端，这将可能导致在故障恢复
时，sink 中有重复的记录。 由于重复的记录将影响下游分析作业的正确性（例如，`SUM`、`COUNT`）， 所以在进一步分析之前需要进行数据去重。

与 Top-N 查询相似，Flink 使用 `ROW_NUMBER()` 去除重复的记录。理论上来说，去重是一个特殊的 Top-N 查询，其中 N 是 1 ，记录则是以处理时间或事件事件进行排序的。

以下代码展示了去重语句的语法：

{% highlight sql %}
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1
{% endhighlight %}

**参数说明：**

- `ROW_NUMBER()`: 从第一行开始，依次为每一行分配一个唯一且连续的号码。
- `PARTITION BY col1[, col2...]`: 指定分区的列，例如去重的键。
- `ORDER BY time_attr [asc|desc]`: 指定排序的列。所制定的列必须为 [时间属性]({{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html)。目前仅支持 [proctime attribute]({{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html#processing-time)，在未来版本中将会支持 [Rowtime atttribute]({{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html#event-time) 。升序（ ASC ）排列指只保留第一行，而降序排列（ DESC ）则指保留最后一行。
- `WHERE rownum = 1`: Flink 需要 `rownum = 1` 以确定该查询是否为去重查询。

以下的例子描述了如何指定 SQL 查询以在一个流计算表中进行去重操作。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// 从外部数据源读取 DataStream
DataStream<Tuple4<String, String, String, Integer>> ds = env.addSource(...);
// 注册名为 “Orders” 的 DataStream
tableEnv.createTemporaryView("Orders", ds, $("order_id"), $("user"), $("product"), $("number"), $("proctime").proctime());

// 由于不应该出现两个订单有同一个order_id，所以根据 order_id 去除重复的行，并保留第一行
Table result1 = tableEnv.sqlQuery(
  "SELECT order_id, user, product, number " +
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) as row_num" +
  "   FROM Orders)" +
  "WHERE row_num = 1");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// 从外部数据源读取 DataStream
val ds: DataStream[(String, String, String, Int)] = env.addSource(...)
// 注册名为 “Orders” 的 DataStream
tableEnv.createTemporaryView("Orders", ds, $"order_id", $"user", $"product", $"number", $"proctime".proctime)

// 由于不应该出现两个订单有同一个order_id，所以根据 order_id 去除重复的行，并保留第一行
val result1 = tableEnv.sqlQuery(
    """
      |SELECT order_id, user, product, number
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) as row_num
      |   FROM Orders)
      |WHERE row_num = 1
    """.stripMargin)
{% endhighlight %}
</div>
</div>

{% top %}

### 分组窗口

SQL 查询的分组窗口是通过 `GROUP BY` 子句定义的。类似于使用常规 `GROUP BY` 语句的查询，窗口分组语句的 `GROUP BY` 子句中带有一个窗口函数为每个分组计算出一个结果。以下是批处理表和流处理表支持的分组窗口函数：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">分组窗口函数</th>
      <th class="text-left">描述</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>TUMBLE(time_attr, interval)</code></td>
      <td>定义一个滚动窗口。滚动窗口把行分配到有固定持续时间（ <code>interval</code> ）的不重叠的连续窗口。比如，5 分钟的滚动窗口以 5 分钟为间隔对行进行分组。滚动窗口可以定义在事件时间（批处理、流处理）或处理时间（流处理）上。</td>
    </tr>
    <tr>
      <td><code>HOP(time_attr, interval, interval)</code></td>
      <td>定义一个跳跃的时间窗口（在 Table API 中称为滑动窗口）。滑动窗口有一个固定的持续时间（ 第二个 <code>interval</code> 参数 ）以及一个滑动的间隔（第一个 <code>interval</code> 参数 ）。若滑动间隔小于窗口的持续时间，滑动窗口则会出现重叠；因此，行将会被分配到多个窗口中。比如，一个大小为 15 分组的滑动窗口，其滑动间隔为 5 分钟，将会把每一行数据分配到 3 个 15 分钟的窗口中。滑动窗口可以定义在事件时间（批处理、流处理）或处理时间（流处理）上。</td>
    </tr>
    <tr>
      <td><code>SESSION(time_attr, interval)</code></td>
      <td>定义一个会话时间窗口。会话时间窗口没有一个固定的持续时间，但是它们的边界会根据 <code>interval</code> 所定义的不活跃时间所确定；即一个会话时间窗口在定义的间隔时间内没有时间出现，该窗口会被关闭。例如时间窗口的间隔时间是 30 分钟，当其不活跃的时间达到30分钟后，若观测到新的记录，则会启动一个新的会话时间窗口（否则该行数据会被添加到当前的窗口），且若在 30 分钟内没有观测到新纪录，这个窗口将会被关闭。会话时间窗口可以使用事件时间（批处理、流处理）或处理时间（流处理）。</td>
    </tr>
  </tbody>
</table>

#### 时间属性

在流处理表中的 SQL 查询中，分组窗口函数的 `time_attr` 参数必须引用一个合法的时间属性，且该属性需要指定行的处理时间或事件时间。可参考 [时间属性文档]({{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html) 以了解如何定义时间属性。

对于批处理的 SQL 查询，分组窗口函数的 `time_attr` 参数必须是一个 `TIMESTAMP` 类型的属性。

#### 选择分组窗口的开始和结束时间戳

可以使用以下辅助函数选择组窗口的开始和结束时间戳以及时间属性：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">辅助函数</th>
      <th class="text-left">描述</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <code>TUMBLE_START(time_attr, interval)</code><br/>
        <code>HOP_START(time_attr, interval, interval)</code><br/>
        <code>SESSION_START(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相对应的滚动、滑动和会话窗口范围内的下界时间戳。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_END(time_attr, interval)</code><br/>
        <code>HOP_END(time_attr, interval, interval)</code><br/>
        <code>SESSION_END(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相对应的滚动、滑动和会话窗口<i>范围以外</i>的上界时间戳。</p>
        <p><b>注意：</b> 范围以外的上界时间戳<i>不可以</i> 在随后基于时间的操作中，作为 <a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html">行时间属性</a> 使用，比如 <a href="#joins">interval join</a> 以及 <a href="#aggregations">分组窗口或分组窗口上的聚合</a>。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相对应的滚动、滑动和会话窗口<i>范围以内</i>的上界时间戳。</p>
      <p>返回的是一个可用于后续需要基于时间的操作的<a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html">时间属性（rowtime attribute）</a>，比如<a href="#joins">interval join</a> 以及 <a href="#aggregations">分组窗口或分组窗口上的聚合</a>。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>返回一个可用于后续需要基于时间的操作的 <a href="{{ site.baseurl }}/zh/dev/table/streaming/time_attributes.html#processing-time">处理时间参数</a>，比如<a href="#joins">interval join</a> 以及 <a href="#aggregations">分组窗口或分组窗口上的聚合</a>.</p></td>
    </tr>
  </tbody>
</table>

*注意：* 辅助函数必须使用与 `GROUP BY` 子句中的分组窗口函数完全相同的参数来调用.

以下的例子展示了如何在流处理表中指定使用分组窗口函数的 SQL 查询。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 从外部数据源读取 DataSource
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);
// 使用“Orders”作为表名把 DataStream 注册为表
tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"), $("proctime").proctime(), $("rowtime").rowtime());

// 计算每日的 SUM(amount)（使用事件时间）
Table result1 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,  " +
  "  SUM(amount) FROM Orders " +
  "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user");

// 计算每日的 SUM(amount)（使用处理时间）
Table result2 = tableEnv.sqlQuery(
  "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");

// 使用事件时间计算过去24小时中每小时的 SUM(amount) 
Table result3 = tableEnv.sqlQuery(
  "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");

// 计算每个以12小时（事件时间）作为不活动时间的会话的 SUM(amount) 
Table result4 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
  "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
  "  SUM(amount) " +
  "FROM Orders " +
  "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// 从外部数据源读取 DataSource
val ds: DataStream[(Long, String, Int)] = env.addSource(...)
// 计算每日（使用处理时间）的 SUM(amount) 
tableEnv.createTemporaryView("Orders", ds, $"user", $"product", $"amount", $"proctime".proctime, $"rowtime".rowtime)

// 计算每日的 SUM(amount) （使用事件时间）
val result1 = tableEnv.sqlQuery(
    """
      |SELECT
      |  user,
      |  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,
      |  SUM(amount)
      | FROM Orders
      | GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user
    """.stripMargin)

// 计算每日的 SUM(amount) （使用处理时间）
val result2 = tableEnv.sqlQuery(
  "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user")

// 使用事件时间计算过去24小时中每小时的 SUM(amount) 
val result3 = tableEnv.sqlQuery(
  "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product")

// 计算每个以12小时（事件时间）作为不活动时间的会话的 SUM(amount) 
val result4 = tableEnv.sqlQuery(
    """
      |SELECT
      |  user,
      |  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart,
      |  SESSION_END(rowtime, INTERVAL '12' HOUR) AS sEnd,
      |  SUM(amount)
      | FROM Orders
      | GROUP BY SESSION(rowtime(), INTERVAL '12' HOUR), user
    """.stripMargin)

{% endhighlight %}
</div>
</div>

{% top %}

### 模式匹配

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">操作符</th>
      <th class="text-center">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>MATCH_RECOGNIZE</strong><br>
        <span class="label label-primary">流处理</span>
      </td>
      <td>
        <p>根据 <code>MATCH_RECOGNIZE</code> <a href="https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip">ISO 标准</a>在流处理表中搜索给定的模式。 这样就可以在SQL查询中描述复杂的事件处理（CEP）逻辑。</p>
        <p>更多详情请参考 <a href="{{ site.baseurl }}/zh/dev/table/streaming/match_recognize.html">检测表中的模式</a>.</p>

{% highlight sql %}
SELECT T.aid, T.bid, T.cid
FROM MyTable
MATCH_RECOGNIZE (
  PARTITION BY userid
  ORDER BY proctime
  MEASURES
    A.id AS aid,
    B.id AS bid,
    C.id AS cid
  PATTERN (A B C)
  DEFINE
    A AS name = 'a',
    B AS name = 'b',
    C AS name = 'c'
) AS T
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}
