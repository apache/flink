---
title: "概念与通用 API"
nav-parent_id: tableapi
nav-pos: 0
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

Table API 和 SQL 被整合在一套共同的 API 中。这套 API 的核心概念是表，用表作为查询操作的输入和输出。本文档介绍了使用 Table API 和 SQL 查询的程序的通用结构、如何注册表、如何查询表以及如何发出表。

* This will be replaced by the TOC
{:toc}

两种计划器的主要区别
-----------------------------------------

1. Blink将批处理作业视为流式处理的特殊情况。因此，也不支持表和数据集之间的转换，批处理作业将不会转换为`DateSet`程序，而是转换为`DataStream`程序，与流作业相同。
2. Blink计划器不支持`BatchTableSource`，而是使用有界的`StreamTableSource`来替代。
3. Blink planner只支持全新的`Catalog`，不支持已弃用的`ExternalCatalog`。
4. 旧计划器和Blink计划器的`FilterableSource`实现不兼容。旧计划器将`PlannerExpression`下推至`filterablesource`，而Blink计划器将`Expression`下推。
5. 基于字符串的键值配置选项（有关详细信息，请参阅有关 [配置]({{ site.baseurl }}/dev/table/config.html)的文档）仅用于Blink planner。
6. 两计划器中的PlannerConfig的实现（`CalciteConfig`）不同。
7. Blink计划器将多个sink优化到一个有向无环图中（仅在`TableEnvironment`上受支持，而在`StreamTableEnvironment`上不受支持）。旧计划器总是将每个sink优化到一个新的有向无环图中，其中所有的有向无环图彼此独立。
8. 旧计划器已不支持catalog统计，但Blink计划器支持。


Table API 和 SQL 程序的结构
---------------------------------------

所有用于批处理和流处理的Table API 和 SQL 程序都遵循相同的模式。下面的代码示例展示了Table API 和 SQL 程序的通用结构。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 为特定计划器的批处理或流处理创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 注册一张表
tableEnv.registerTable("table1", ...)            // 或者
tableEnv.registerTableSource("table2", ...);
// 注册一张输出表
tableEnv.registerTableSink("outputTable", ...);

// 通过Table API查询创建表
Table tapiResult = tableEnv.scan("table1").select(...);
// 通过SQL查询创建表
Table sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");

// 发出一张Table API结果表，SQL结果方法相同
tapiResult.insertInto("outputTable");

// 执行
tableEnv.execute("java_job");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 为特定计划器的批处理或流处理创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 注册一张表
tableEnv.registerTable("table1", ...)           // or
tableEnv.registerTableSource("table2", ...)
// 注册一张输出表
tableEnv.registerTableSink("outputTable", ...);

// 通过Table API查询创建表
val tapiResult = tableEnv.scan("table1").select(...)
// 通过SQL查询创建表
val sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ...")

// 发送一张Table API结果表，SQL结果方法相同
tapiResult.insertInto("outputTable")

// 执行
tableEnv.execute("scala_job")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

# 为特定计划器的批处理或流处理创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

# 注册一张表
table_env.register_table("table1", ...)           # 或
table_env.register_table_source("table2", ...)

# 注册一张输出表
table_env.register_table_sink("outputTable", ...);

# 通过Table API查询创建表
tapi_result = table_env.scan("table1").select(...)
# 通过SQL查询创建表
sql_result  = table_env.sql_query("SELECT ... FROM table2 ...")

# 发送一张 Table API 结果表，SQL 结果方法相同
tapi_result.insert_into("outputTable")

# 执行
table_env.execute("python_job")

{% endhighlight %}
</div>
</div>

**注释:** Table API 和 SQL 查询可以被很容易地整合并嵌入DataStream或DataSet程序中。查看 [与DataStream和DataSet API 结合](#integration-with-datastream-and-dataset-api)章节，了解如何将DataStream和DataSet转换为表，反之亦然。

{% top %}

创建 TableEnvironment
-------------------------

`TableEnvironment`是 Table API 和 SQL 整合的核心概念。 它负责:

* 在internal catalog中注册一张 `Table` 
* 注册一个 external catalog 
* 执行 SQL 查询
* 注册一个用户自定义的 (scalar, table, 或者 aggregation) function
* 将一个 `DataStream` 或者 `DataSet` 转换成一张 `Table`
* 持有对`ExecutionEnvironment`或`StreamExecutionEnvironment`的引用；

`Table`总是绑定到特定的`TableEnvironment`。不可能在同一查询中组合不同表环境的表，例如联接或联合它们。

通过`StreamExecutionEnvironment`或`ExecutionEnvironment`和可选的`TableConfig`调用静态方法`BatchTableEnvironment.create()`或`StreamTableEnvironment.create()`来创建`TableEnvironment`。 `TableConfig`可用于配置`TableEnvironment`或自定义查询优化和转换过程(参阅 [查询优化](#query-optimization)).

请确保选择与您的编程语言匹配的特定计划器`BatchTableEnvironment`/`StreamTableEnvironment`。

如果两种计划器 jar 都在类路径上（默认行为），则应显式设置要在当前程序中使用的计划器。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// **********************
// FLINK 流查询
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
// or TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);

// ******************
// FLINK 批处理查询
// ******************
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

// **********************
// BLINK 流查询
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
// or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

// ******************
// BLINK 批处理查询
// ******************
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// **********************
// FLINK 流查询
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
// or val fsTableEnv = TableEnvironment.create(fsSettings)

// ******************
// FLINK 批处理查询
// ******************
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

val fbEnv = ExecutionEnvironment.getExecutionEnvironment
val fbTableEnv = BatchTableEnvironment.create(fbEnv)

// **********************
// BLINK 流查询
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
// or val bsTableEnv = TableEnvironment.create(bsSettings)

// ******************
// BLINK 批处理查询
// ******************
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
val bbTableEnv = TableEnvironment.create(bbSettings)

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

# **********************
# FLINK 流查询
# **********************
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

f_s_env = StreamExecutionEnvironment.get_execution_environment()
f_s_settings = EnvironmentSettings.new_instance().use_old_planner().in_streaming_mode().build()
f_s_t_env = StreamTableEnvironment.create(f_s_env, environment_settings=f_s_settings)

# ******************
# FLINK 批处理查询
# ******************
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment

f_b_env = ExecutionEnvironment.get_execution_environment()
f_b_t_env = BatchTableEnvironment.create(f_b_env, table_config)

# **********************
# BLINK 流查询
# **********************
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

b_s_env = StreamExecutionEnvironment.get_execution_environment()
b_s_settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
b_s_t_env = StreamTableEnvironment.create(b_s_env, environment_settings=b_s_settings)

# ******************
# BLINK 批处理查询
# ******************
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

b_b_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
b_b_t_env = BatchTableEnvironment.create(environment_settings=b_b_settings)

{% endhighlight %}
</div>
</div>

**注释:** 如果`/lib`目录中只有一个 planner jar，则可以使用`useAnyPlanner`(python的`use_any_planner`)创建特定的`EnvironmentSettings`。


{% top %}

在 Catalog 中注册表
-------------------------------

TableEnvironment维护按名称注册的表的catalog。有两种类型的表, *输入表* 和 *输出表*.。输入表可以在 Table API 和 SQL查询中引用并提供输入数据。输出表可用于将 Table API 或 SQL查询的结果发送到外部系统。

输入表可以从多种来源注册：

* 一个现有的`Table`对象，通常是 Table API 或 SQL 查询的结果。
* 一种访问外部数据，如文件、数据库或消息传递系统，的`TableSource`，。
* 来自数据流（仅限于流作业）或数据集（仅限于从旧计划器转换的批处理作业）程序的`DataStream`或`DataSet`。注册 `DataStream` 或 `DataSet` 在 [与 DataStream 和 DataSet API 结合](#integration-with-datastream-and-dataset-api) 章节中讨论。

可以使用`TableSink`注册输出表。

### 注册表

`Table`在`TableEnvironment`中注册方法如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 表是简单投影查询的结果
Table projTable = tableEnv.scan("X").select(...);

// 将表projTable注册为表“projectedTable”
tableEnv.registerTable("projectedTable", projTable);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 表是简单投影查询的结果
val projTable: Table = tableEnv.scan("X").select(...)

// 将表projTable注册为表“projectedTable”
tableEnv.registerTable("projectedTable", projTable)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

# 表是简单投影查询的结果
proj_table = table_env.scan("X").select(...)

# 将表projTable注册为表“projectedTable”
table_env.register_table("projectedTable", proj_table)
{% endhighlight %}
</div>
</div>

**注释:** 注册的“Table”与关系数据库系统中的“VIEW”处理方式类似，即定义“Table”的查询没有经过优化，但当另一个查询引用注册的“Table”时将被内联。如果多个查询引用同一个已注册的“表”，则它将被每个引用查询内联并多次执行，即已注册的“表”的结果将*不*共享。

{% top %}

### 注册 TableSource

`TableSource`提供对存储在存储系统（如数据库（MySQL、HBase，…）、具有特定编码的文件（CSV、Apache \[Parquet、Avro、ORC \]，…）或消息传递系统（Apache Kafka、RabbitMQ，…）中的外部数据的访问。

Flink旨在为常见的数据格式和存储系统提供TableSource。 请参阅 [Table Sources 和 Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) 页以获取受支持 的 TableSource列表以及有关如何生成自定义`TableSource`的说明。

`TableSource`在`TableEnvironment`中注册如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 创建TableSource
TableSource csvSource = new CsvTableSource("/path/to/file", ...);

// 将TableSource注册为表“CsvTable”
tableEnv.registerTableSource("CsvTable", csvSource);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 创建TableSource
val csvSource: TableSource = new CsvTableSource("/path/to/file", ...)

// 将TableSource注册为表“CsvTable”
tableEnv.registerTableSource("CsvTable", csvSource)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节


# 创建TableSource
csv_source = CsvTableSource("/path/to/file", ...)

# 将TableSource注册为表“CsvTable”
table_env.register_table_source("csvTable", csv_source)
{% endhighlight %}
</div>
</div>

**注释:**用于Blink计划器的`TableEnvironment`只接受`StreamTableSource`、`LookupableTableSource`和`InputFormatTableSource`，用于批处理Blink计划器的`StreamTableSource`必须有界。

{% top %}

### 注册TableSink

注册的TableSink可以用于向外部存储系统 [发出Table API 或 SQL 的查询结果](common.html#emit-a-table)，例如数据库、键值存储、消息队列或文件系统（在不同的编码中，例如CSV、Apache\[Parquet、Avro、ORC\]，…）。

Flink旨在为常见的数据格式和存储系统提供TableSink。请参阅 [Table Sources 和 Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) 章节以获取有关可用TableSource的详细信息以及如何实现自定义`TableSink`的说明。

`TableSource`在`TableEnvironment`中的注册如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 创建TableSink
TableSink csvSink = new CsvTableSink("/path/to/file", ...);

// 定义字段名称和类型
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};

// 将TableSink注册为表“CsvSinkTable”
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节


// 创建TableSink
val csvSink: TableSink = new CsvTableSink("/path/to/file", ...)

// 定义字段名称和类型
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.LONG)

// 将TableSink注册为表“CsvSinkTable”
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

# 定义字段名称和类型
field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# 创建TableSink
csv_sink = CsvTableSink(field_names, field_types, "/path/to/file", ...)

# 将TableSink注册为表“CsvSinkTable”
table_env.register_table_sink("CsvSinkTable", csv_sink)
{% endhighlight %}
</div>
</div>

{% top %}

查询表 
-------------

### Table API

Table API是Scala和Java的语言集成式的查询API。与SQL不同，查询语句不指定为字符串，而是在宿主语言中一步一步组成的。

API基于`Table`类，该类表示表（流式处理或批处理），并提供方法使用关系操作。  这些方法返回一个新的`Table`对象，该对象表示对输入`Table`使用关系操作的结果。有些关系操作由多个方法调用组成，如`table.groupBy(…).select()`，其中`groupBy(…)`指定“table”的分组，以及`select(…)`指定“table”分组上的投影。

[Table API]({{ site.baseurl }}/dev/table/tableApi.html) 文档描述了流式处理和批处理表上支持的所有 Table API 操作。

以下示例展示了一个简单的“Table API”聚合查询：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节


// 注册Orders表

// 扫描注册的Orders表
Table orders = tableEnv.scan("Orders");
// 计算法国所有客户的收入
Table revenue = orders
  .filter("cCountry === 'FRANCE'")
  .groupBy("cID, cName")
  .select("cID, cName, revenue.sum AS revSum");

// 发出或转换表
// 执行查询
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 注册Orders表

// 扫描注册的Orders表
val orders = tableEnv.scan("Orders")
// 计算法国所有客户的收入
val revenue = orders
  .filter('cCountry === "FRANCE")
  .groupBy('cID, 'cName)
  .select('cID, 'cName, 'revenue.sum AS 'revSum)

// 发出或转换表
// 执行查询
{% endhighlight %}

**注释:** Scala Table API使用Scala符号（以单个记号(`'`）开头）来引用`Table`的属性。The Table API使用了Scala隐格式。请确保导入`org.apache.flink.api.scala._`和`org.apache.flink.table.api.scala._`，以便使用scala隐式转换。
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = # 请参阅“创建TableEnvironment”章节

# 注册Orders表

# 扫描注册的Orders表
orders = table_env.scan("Orders")
# 计算法国所有客户的收入
revenue = orders \
    .filter("cCountry === 'FRANCE'") \
    .group_by("cID, cName") \
    .select("cID, cName, revenue.sum AS revSum")

# 发出或转换表
# 执行查询
{% endhighlight %}
</div>
</div>

{% top %}

### SQL

Flink的SQL集成是基于 [Apache Calcite](https://calcite.apache.org)的， 它实现了SQL标准。SQL查询语句被指定为常规字符串。

[SQL]({{ site.baseurl }}/dev/table/sql.html) 文档描述了Flink SQL对流式处理和批处理表的支持。

下面的示例演示如何指定查询并将结果返回为`Table`。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 注册Orders表

// 计算法国所有客户的收入
Table revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );

// 发出或转换表
// 执行查询
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 注册Orders表

// 计算法国所有客户的收入
val revenue = tableEnv.sqlQuery("""
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

// 发出或转换表
// 执行查询
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

# 注册Orders表

# 计算法国所有客户的收入
revenue = table_env.sql_query(
    "SELECT cID, cName, SUM(revenue) AS revSum "
    "FROM Orders "
    "WHERE cCountry = 'FRANCE' "
    "GROUP BY cID, cName"
)

# 发出或转换表
# 执行查询
{% endhighlight %}
</div>
</div>

下面的示例演示如何指定将其结果插入已注册表的更新查询。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 注册Orders表
// 注册 "RevenueFrance" 输出表

// 计算来自法国的所有客户的收入并将其放入到 "RevenueFrance"
tableEnv.sqlUpdate(
    "INSERT INTO RevenueFrance " +
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );

// 执行查询
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 注册Orders表
// 注册 "RevenueFrance" 输出表

// 计算来自法国的所有客户的收入并将其放入到 "RevenueFrance"
tableEnv.sqlUpdate("""
  |INSERT INTO RevenueFrance
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

// 执行查询
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

# 注册Orders表
# 注册 "RevenueFrance" 输出表

# 计算来自法国的所有客户的收入并将其放入到 "RevenueFrance"
table_env.sql_update(
    "INSERT INTO RevenueFrance "
    "SELECT cID, cName, SUM(revenue) AS revSum "
    "FROM Orders "
    "WHERE cCountry = 'FRANCE' "
    "GROUP BY cID, cName"
)

# 执行查询
{% endhighlight %}
</div>
</div>

{% top %}

### 混合Table API 和 SQL

Table API 和 SQL 查询很容易混合，因为它们都返回`Table`对象：

* 可以在SQL查询返回的“Table”对象上定义Table API查询。
* 通过在“TableEnvironment”中[注册结果表](#register-a-table)并在SQL查询的“FROM”子句中引用它，可以在Table API查询的结果上定义SQL查询。

{% top %}

发出表 
------------

`Table`是通过将其写入`TableSink`而发出的。`TableSink`是一个通用接口，用于支持多种文件格式（如CSV、Apache Parquet、Apache Avro）、存储系统（如JDBC、Apache HBase、Apache Cassandra、Elasticsearch）或消息传递系统（如Apache Kafka、RabbitMQ）。

批处理`Table`只能写入`BatchTableSink`，而流式处理`Table`需要`AppendStreamTableSink`、`RetractStreamtableSink`或`UpsertStreamTableSink`。

请参阅文档 [Table Sources 和 Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) 获取有关可用接收器的详细信息以及如何实现自定义`TableSink`的说明。

`Table.insertInto(String tableName)`方法将`Table`发出到已注册的`TableSink`中。该方法通过名称从catalog中查找`TableSink`，并验证` Table`的结构与`TableSink`的结构相同。

以下示例展示了如何发出`Table`：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建TableEnvironment
TableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 创建TableSink
TableSink sink = new CsvTableSink("/path/to/file", fieldDelim = "|");

// 使用指定结构注册`TableSink`
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);

// 使用Table API算子和/或SQL查询来计算结果表
Table result = ...
// 将结果表发送到已注册的TableSink
result.insertInto("CsvSinkTable");

// 执行程序
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

// 创建TableSink
val sink: TableSink = new CsvTableSink("/path/to/file", fieldDelim = "|")

// 使用指定结构注册`TableSink`
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation] = Array(Types.INT, Types.STRING, Types.LONG)
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink)

// 使用Table API算子和/或SQL查询来计算结果表
val result: Table = ...

// 将结果表发送到已注册的TableSink
result.insertInto("CsvSinkTable")

// 执行程序
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建TableEnvironment
table_env = ... # 请参阅“创建TableEnvironment”章节

field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# 创建TableSink
sink = CsvTableSink(field_names, field_types, "/path/to/file", "|")

table_env.register_table_sink("CsvSinkTable", sink)

# 使用Table API算子和/或SQL查询来计算结果表
result = ...

# 将结果表发送到已注册的TableSink
result.insert_into("CsvSinkTable")

# 执行程序
{% endhighlight %}
</div>
</div>

{% top %}


解释并执行查询
-----------------------------

对于两个计划器来说，解释和执行查询的方法是不同的。

<div class="codetabs" markdown="1">
<div data-lang="Old planner" markdown="1">
Table API和SQL查询转换为[DataStream]（{{site.baseurl}} / dev / datastream_api.html）或[DataSet]（{{site.baseurl}} / dev / batch）程序取决于它们的输入是否是流或批处理输入。 查询在内部表示为逻辑查询计划，并分为两个阶段：

1. 优化逻辑计划
2. 转换为DataStream或DataSet程序

在以下情况下，Table API或SQL查询将被转换：

* 一个`Table`被发送给`TableSink`，即当调用`Table.insertInto()`时。
* 指定了SQL更新查询，即在调用`TableEnvironment.sqlUpdate()`时。
* 一个 `Table` 被转化成一个 `DataStream` 或 `DataSet` (参见 [与 DataStream 和 DataSet API 结合](#integration-with-datastream-and-dataset-api)).

转换后，将像常规的DataStream或DataSet程序一样处理Table API或SQL查询，并在调用`StreamExecutionEnvironment.execute()`或`ExecutionEnvironment.execute()`时执行。

</div>

<div data-lang="Blink planner" markdown="1">
无论Table API和SQL查询的输入是流式处理还是批处理，都将转换为[DataStream]（{{site.baseurl}} / dev / datastream_api.html）程序。

1. 优化逻辑计划，
2. 转换为DataStream程序。

`TableEnvironment`和`StreamTableEnvironment`解释查询的方法不同。

对于`TableEnvironment`，在调用`TableEnvironment.execute()`时Table API或SQL查询将被转换，因为TableEnvironment会将多个接收器优化为一个DAG。

对于`StreamTableEnvironment`，在以下情况下Table API或SQL查询会被转换：

* 一个`Table`被发送给`TableSink`，即当调用`Table.insertInto()`时。
* 指定了SQL更新查询，即在调用`TableEnvironment.sqlUpdate()`时。
* 一个`Table`被转换成一个`DataStream`。

转换后，将像常规DataStream程序一样处理Table API或SQL查询，并在调用`TableEnvironment.execute()`或`StreamExecutionEnvironment.execute()`时执行。

</div>
</div>

{% top %}

与 DataStream 和 DataSet API 结合
-------------------------------------------

两个流处理计划器都可以与`DataStream API`结合。 只有旧计划器能与 `DataSet API`结合, Blink批处理计划器两者都不能结合。
**注释:** 下面讨论的“ DataSet” API仅与旧批处理计划器有关。

Table API 和 SQL 查询 可以被轻松的集成并嵌入到 [DataStream]({{ site.baseurl }}/dev/datastream_api.html) 和 [DataSet]({{ site.baseurl }}/dev/batch) 程序中。例如，可以查询外部表（例如从RDBMS），进行一些预处理，例如过滤，投影，聚合或与元数据联接，然后使用DataStream或 DataSet API（以及在这些API之上构建的任何库，例如CEP或Gelly）。相反，也可以将Table API或SQL查询应用于DataStream或DataSet程序的结果。

可以通过将`DataStream`或`DataSet`转换为`Table`，反之亦然来实现这种交互。 在本节中，我们描述如何完成这些转换。

### Scala的隐式转换

Scala Table API具有对`DataSet`，`DataStream`和`Table`类的隐式转换。通过为Scala DataStream API导入`org.apache.flink.table.api.scala._`包以及`org.apache.flink.api.scala._`包，可以启用这些转换。

### 将DataStream或DataSet注册为表

在`TableEnvironment`中,可以将`DataStream`或`DataSet`作为表注册。结果表的结构取决于已注册的`DataStream`或`DataSet`的数据类型。请参阅 [数据类型到表结构的映射](#mapping-of-data-types-to-table-schema) 章节以获取细节。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment
// 在BatchTableEnvironment中注册DataSet是相同的
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将DataStream注册为具有字段“ f0”，“ f1”的表“ myTable”
tableEnv.registerDataStream("myTable", stream);

// 使用字段“ myLong”，“ myString”将DataStream注册为表“ myTable2”
tableEnv.registerDataStream("myTable2", stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment 
// DataSet的注册是相同的
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[(Long, String)] = ...

// 将DataStream注册为具有字段“ f0”，“ f1”的表“ myTable”
tableEnv.registerDataStream("myTable", stream)

// 使用字段“ myLong”，“ myString”将DataStream注册为表“ myTable2”
tableEnv.registerDataStream("myTable2", stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

**注释:** DataStream表的名称不得与`^_DataStreamTable_[0-9]+`模式匹配，DataSet表的名称不得与`^_DataSetTable_[0-9]+`模式匹配。 这些模式仅供内部使用。

{% top %}

### 将DataStream或DataSet转换为表

除了在`TableEnvironment`中注册`DataStream`或`DataSet`之外，还可以将其直接转换为`Table`。如果要在Table API查询中使用Table，这将很方便。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment
// 在BatchTableEnvironment中注册DataSet是相同的
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将DataStream转换为带有默认字段“ f0”，“ f1”的表
Table table1 = tableEnv.fromDataStream(stream);

// 将DataStream转换为具有字段“ myLong”，“ myString”的表
Table table2 = tableEnv.fromDataStream(stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
// DataSet的注册是相同的
val tableEnv = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[(Long, String)] = ...

// 将DataStream转换为具有默认字段'_1，'_2的表
val table1: Table = tableEnv.fromDataStream(stream)

// 将DataStream转换为具有字段'myLong，'myString的表
val table2: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

{% top %}

### 将表转换为DataStream或DataSet

`Table`可被转换成一个`DataStream`或`DataSet`。 这样，可以在Table API或SQL查询的结果上运行自定义DataStream或DataSet程序。

在将 `Table` 转换成`DataStream`或者`DataSet`时， 您需要指定生成的`DataStream`或`DataSet`的数据类型，即，`Table`的Row要转换成的数据类型。最方便的转换类型通常是`Row`。 以下列表概述了不同选项的功能：

- **Row**: 字段是按位置映射的，字段的数量是任意的，支持“null”值，非安全访问类型。
- **POJO**: 字段按名称映射（POJO字段必须命名为“Table”字段）、任意数量的字段、对“null”值的支持、安全访问类型。
- **Case Class**: 字段按位置映射，不支持“null”值，类型安全访问。
- **Tuple**: 字段按位置映射，限制为22个（Scala）或25个（Java）字段，不支持“null”值，安全访问类型。
- **Atomic Type**: `Table`必须有一个字段，不支持'null'值，安全访问类型。

#### 将表转换为DataStream

流式查询的结果`Table`将动态更新，即， 当新记录到达查询的输入流时，它将发生变化。因此，将这种动态查询转换成的`DataStream`需要对表的更新进行编码。

将`Table`转换为`DataStream`有两种模式：

1. **Append Mode**: 仅当动态`Table`仅由`INSERT`更改进行修改（即，仅追加且以前发出的结果从不更新）时，才能使用此模式。
2. **Retract Mode**: 此模式始终可用。它使用“boolean”标志对“INSERT”和“DELETE”更改进行编码。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment. 
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 带两个字段的表（字符串名称、整数年龄）
Table table = ...

// 通过指定类将表转换为Row的Append DataStream
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

// 通过类型信息将表转换为Tuple2<String，Integer>的Append DataStream
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple = 
  tableEnv.toAppendStream(table, tupleType);

// 将表转换为Row的Retract DataStream。
//  X类型的Retract Stream是一个DataStream<Tuple2<Boolean，X>>。
//   布尔字段表示更改的类型。
//   True是INSERT，false是DELETE。
DataStream<Tuple2<Boolean, Row>> retractStream = 
  tableEnv.toRetractStream(table, Row.class);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment. 
// DataSet的注册相同
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

// 带两个字段的表 (String name, Integer age)
val table: Table = ...

// 将表转换为Row的Append DataStream
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// 将表转换为Tuple2的Append DataStream[String，Int]
val dsTuple: DataStream[(String, Int)] dsTuple = 
  tableEnv.toAppendStream[(String, Int)](table)

// 将表转换为Row的Retract DataStream。
//   X类型的Retract Stream是一个DataStream[(Boolean, X)]. 
//   布尔字段表示更改的类型。
//   True是INSERT，false是DELETE。
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
{% endhighlight %}
</div>
</div>

**注释:** [动态表]（streaming / dynamic_tables.html）文档中提供了有关动态表及其属性的详细讨论。

#### 将表转换为DataSet

将`Table`转换为`DataSet`，如下所示：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建BatchTableEnvironment
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

// 带两个字段的表 (String name, Integer age)
Table table = ...

// 通过指定一个类将表转换为Row的DataSet
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

// 通过TypeInformation将表转换为Tuple2 <String，Integer>的DataSet
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple = 
  tableEnv.toDataSet(table, tupleType);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment 
//DataSet的注册是相同的
val tableEnv = BatchTableEnvironment.create(env)

// 带两个字段的表 (String name, Integer age)
val table: Table = ...

// 通过指定一个类将表转换为Row的DataSet
val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](table)

// 将表转换为Tuple2 [String，DataSet
val dsTuple: DataSet[(String, Int)] = tableEnv.toDataSet[(String, Int)](table)
{% endhighlight %}
</div>
</div>

{% top %}

### 数据类型到表结构的映射

Flink的DataStream和DataSet API支持非常多种类型。元组（内置Scala和Flink Java元组），POJO，Scala样例类（case class）和Flink的Row类型等复合类型允许嵌套具有多个可在表表达式中使用的字段的数据结构。其他类型被视为原子类型。下面，我们将叙述Table API如何将这些类型转换为内部row表示形式，并展示将DataStream转换为Table的示例。

数据类型到表结构的映射可以通过两种方式进行： **基于字段位置** 或 **基于字段名称**.

**基于位置的映射**

基于位置的映射可用于在保持字段顺序的同时为字段提供更有意义的名称。此映射可用于*具有定义的字段顺序*的复合数据类型以及原子类型。元组，Row和样例类等复合数据类型具有这样的字段顺序。但是，必须根据字段名称映射POJO的字段（请参阅下一节）。可以将字段映射出来，但不能使用“ as”重命名。

定义基于位置的映射时，输入数据类型中一定不能存在指定的名称，否则API会认为应该基于字段名称进行映射。如果未指定任何字段名称，则使用默认的字段名称和复合类型的字段顺序，或者使用“ f0”表示原子类型。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节;

DataStream<Tuple2<Long, Integer>> stream = ...

// 将DataStream转换为具有默认字段名称“ f0”和“ f1”的表
Table table = tableEnv.fromDataStream(stream);

// 仅将数据流转换为具有字段“ myLong”的表
Table table = tableEnv.fromDataStream(stream, "myLong");

// 将DataStream转换为具有字段名称“ myLong”和“ myInt”的表
Table table = tableEnv.fromDataStream(stream, "myLong, myInt");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[(Long, Int)] = ...

// 将DataStream转换为具有默认字段名称“ _1”和“ _2”的表
val table: Table = tableEnv.fromDataStream(stream)

// 仅将数据流转换为具有字段“ myLong”的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong)

// 
67/5000
将DataStream转换为具有字段名称“ myLong”和“ myInt”的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myInt)
{% endhighlight %}
</div>
</div>

**基于名称的映射**

基于名称的映射可用于任何数据类型，包括POJO。这是定义表结构映射的最灵活的方法。投影中的所有字段均按名称引用，并且可以使用“ as”重命名。字段可以重新排序和投影。

如果未指定字段名，则使用组合类型的默认字段名和字段顺序，或者使用“f0”命名原子类型。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

DataStream<Tuple2<Long, Integer>> stream = ...

// 将DataStream转换为具有默认字段名“f0”和“f1”的表
Table table = tableEnv.fromDataStream(stream);

// 将DataStream转换为仅包含字段“f1”的表
Table table = tableEnv.fromDataStream(stream, "f1");

// 将DataStream转换为具有交换字段的表
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// 将数据流转换为具有交换字段和字段名“myInt”和“myLong”的表
Table table = tableEnv.fromDataStream(stream, "f1 as myInt, f0 as myLong");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[(Long, Int)] = ...

// 将DataStream转换为具有默认字段名“1”和“2”的表
val table: Table = tableEnv.fromDataStream(stream)

// 将数据流转换为字仅有段“_2”的表
val table: Table = tableEnv.fromDataStream(stream, '_2)

// 将DataStream转换为具有交换字段的表
val table: Table = tableEnv.fromDataStream(stream, '_2, '_1)

// 将数据流转换为具有交换字段和字段名“myInt”和“myLong”的表
val table: Table = tableEnv.fromDataStream(stream, '_2 as 'myInt, '_1 as 'myLong)
{% endhighlight %}
</div>
</div>

#### 原子类型

Flink将原生类型（`Integer`，`Double`，`String`）或泛型类型（无法分析和分解的类型）视为原子类型。原子类型的`DataStream`或`DataSet`转换为具有单个属性的`Table`。属性的类型是从原子类型推断出来的，并且可以指定属性的名称。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

DataStream<Long> stream = ...

// 将DataStream转换为默认字段名为“f0”的表
Table table = tableEnv.fromDataStream(stream);

// 将DataStream转换为字段名为“myLong”的表
Table table = tableEnv.fromDataStream(stream, "myLong");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[Long] = ...

// 将DataStream转换为默认字段名为“f0”的表
val table: Table = tableEnv.fromDataStream(stream)

// 将DataStream转换为字段名为“myLong”的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong)
{% endhighlight %}
</div>
</div>

#### 元组（Scala和Java）和样例类（仅限Scala）

Flink支持Scala的内置元组，并为Java提供了自己的元组类。这两种元组的DataSet和DataStream都可以转换为表。通过为所有字段提供名称（基于位置的映射），可以重命名字段。如果未指定字段名，则使用默认字段名。如果引用了原始字段名称（对于Flink元组为`f0`，`f1` ... ...，对于Scala元组为`_1`，`_2` ... ...），则API会假定映射是基于名称的而不是基于位置的。基于名称的映射允许使用别名（“ as”）对字段和投影进行重新排序。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将DataStream转换为具有默认字段名称“ f0”，“ f1”的表
Table table = tableEnv.fromDataStream(stream);

// 使用重命名的字段名称“ myLong”，“ myString”将DataStream转换为表（基于位置）
Table table = tableEnv.fromDataStream(stream, "myLong, myString");

// 使用重新排序的字段“ f1”，“ f0”将DataStream转换为表（基于名称）
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// 将DataStream转换为带有投影字段“ f1”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "f1");

// 使用重新排序和重命名的字段“ myString”，“ myLong”将DataStream转换为表（基于名称）
Table table = tableEnv.fromDataStream(stream, "f1 as 'myString', f0 as 'myLong'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节

val stream: DataStream[(Long, String)] = ...

// 将DataStream转换为具有重命名的默认字段名'\_1，'\_2的表
val table: Table = tableEnv.fromDataStream(stream)

// 将DataStream转换为字段名为“myLong”、“myString”的表（基于位置）
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)

// 将DataStream转换为具有重新排序字段“\_2”、“\_1”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, '\_2, '\_1)

// 将DataStream转换为具有投影字段“\_2”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, '_2)

// 将DataStream转换为具有重新排序和别名字段“myString”、“myLong”（基于名称）的表
val table: Table = tableEnv.fromDataStream(stream, '\_2 as 'myString, '_1 as 'myLong)

// 定义样例类
case class Person(name: String, age: Int)
val streamCC: DataStream[Person] = ...

// 将DataStream转换为具有默认字段名“name“，“age“的表
val table = tableEnv.fromDataStream(streamCC)

// 将DataStream转换为字段名为“myName“，“myAge“的表（基于位置）
val table = tableEnv.fromDataStream(streamCC, 'myName, 'myAge)

// 将DataStream转换为具有重新排序和别名字段“myAge”、“myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

{% endhighlight %}
</div>
</div>

#### POJO（Java和Scala）

Flink支持POJO作为复合类型。确定POJO的规则记录在[这里]({{ site.baseurl }}/dev/api_concepts.html#pojos).

在不指定字段名称的情况下将POJO DataStream或DataSet转换为Table时，将使用原始POJO字段的名称。名称映射需要原始名称，并且不能按位置进行。字段可以使用别名（使用`as`关键字）来重命名，重新排序和投影。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// Person是具有字段“name”和“age”的POJO
DataStream<Person> stream = ...

// 将DataStream转换为具有默认字段名称“ age”，“ name”的表（字段按名称排序！）
Table table = tableEnv.fromDataStream(stream);

//将DataStream转换为具有重命名字段“ myAge”，“ myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "age as myAge, name as myName");

// 将DataStream转换为具有映射字段“name”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name");

// 将DataStream转换为具有映射和重命名字段“ myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节
// Person是具有字段“name”和“age”的POJO
val stream: DataStream[Person] = ...

// 将DataStream转换为具有默认字段名称“ age”，“ name”的表（字段按名称排序！）
val table: Table = tableEnv.fromDataStream(stream)

// 将DataStream转换为具有重命名字段“ myAge”，“ myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

// 将DataStream转换为具有映射字段“name”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name)

// 将DataStream转换为具有映射和重命名字段“ myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

#### Row

`Row`数据类型支持任意数量的字段和具有`null`值的字段。字段名称可以通过`RowTypeInfo`指定，也可以在将` Row`` DataStream`或`DataSet`转换为`Table`时指定。Row类型支持按位置和名称映射字段。可以通过提供所有字段的名称（基于位置的映射）来重命名字段，也可以为投影/排序/重命名（基于名称的映射）单独选择字段。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建StreamTableEnvironment，BatchTableEnvironment操作相同
StreamTableEnvironment tableEnv = ...; // 请参阅“创建TableEnvironment”章节

// 在`RowTypeInfo`中指定了两个字段“ name”和“ age”的Row的DataStream
DataStream<Row> stream = ...

// 将DataStream转换为具有默认字段名称“ age”，“ name”的表
Table table = tableEnv.fromDataStream(stream);

// 将DataStream转换为具有映射字段“ myAge”，“ myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "myName, myAge");

// 将DataStream转换为具有重命名字段“ myAge”，“ myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName, age as myAge");

// 将DataStream转换为具有映射字段“name”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name");

// 将DataStream转换为具有映射和重命名字段“ myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 请参阅“创建TableEnvironment”章节
// 在`RowTypeInfo`中指定了两个字段“ name”和“ age”的Row的DataStream
val stream: DataStream[Row] = ...

// 使用默认字段名称“ name”，“ age”将DataStream转换为表
val table: Table = tableEnv.fromDataStream(stream)

// 使用重命名的字段名称“ myName”，“ myAge”将DataStream转换为表（基于位置）
val table: Table = tableEnv.fromDataStream(stream, 'myName, 'myAge)

// 使用重命名字段“ myName”，“ myAge”将DataStream转换为表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName, 'age as 'myAge)

// 将DataStream转换为具有映射字段“name”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name)

// 将DataStream转换为具有映射和重命名字段“ myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

{% top %}


查询优化
------------------

<div class="codetabs" markdown="1">
<div data-lang="Old planner" markdown="1">

Apache Flink使用Apache Calcite来优化和翻译查询。当前执行的优化包括投影和过滤器下推，子查询去相关以及其他类型的查询重写。旧计划器尚未优化join的顺序，而是按照查询中定义的顺序执行它们（FROM子句中的表顺序和/或WHERE子句中的join谓词顺序）。

通过提供一个CalciteConfig对象，可以调整在不同阶段应用的优化规则集。这可以通过构建器调用`CalciteConfig.createBuilder()`来创建，并通过调用`tableEnv.getConfig.setPlannerConfig(calciteConfig)`提供给TableEnvironment。

</div>

<div data-lang="Blink planner" markdown="1">
Apache Flink利用并扩展了Apache Calcite来执行复杂的查询优化。
这包括一系列基于规则和成本的优化，例如：

* 基于Apache Calcite的子查询解相关
* 项目修剪
* 分区修剪
* 过滤器下推
* 子计划重复数据删除避免重复计算
* 特殊的子查询重写，包括两个部分：
    * 将IN和EXISTS转换为left semi-joins
    * 将NOT IN和NOT EXISTS转换为left anti-join
* Optional join重新排序
    * 通过`table.optimizer.join-reorder-enabled`启用

**注释:** IN/EXISTS/NOT IN/NOT EXISTS当前仅在子查询重写中的连接条件中受支持。

优化器不仅基于计划，还基于数据源的丰富统计数据和每个运营商（如io、cpu、网络和内存）的细粒度成本，做出智能决策。

高级用户可以通过`CalciteConfig`对象提供自定义优化，该对象可以通过调用`table environment#getConfig#setPlannerConfig`提供给表环境。

</div>
</div>


### 解释表

Table API提供了一种机制来解释计算`Table`的逻辑和优化查询计划。
这是通过`TableEnvironment.explain(table)`方法或`TableEnvironment.explain()`方法完成的。`explain(table)`返回给定`Table`的计划。 `explain()`返回多接收器计划的结果，主要用于Blink计划器。它返回一个字符串，描述三个计划：

1. 关系查询的抽象语法树，即未优化的逻辑查询计划，
2. 优化的逻辑查询计划，以及
3. 物理执行计划。

以下代码使用`explain(Table)`，显示给定`Table`的示例和相应输出：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

Table table1 = tEnv.fromDataStream(stream1, "count, word");
Table table2 = tEnv.fromDataStream(stream2, "count, word");
Table table = table1
  .where("LIKE(word, 'F%')")
  .unionAll(table2);

String explanation = tEnv.explain(table);
System.out.println(explanation);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table = table1
  .where('word.like("F%"))
  .unionAll(table2)

val explanation: String = tEnv.explain(table)
println(explanation)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

table1 = t_env.from_elements([(1, "hello")], ["count", "word"])
table2 = t_env.from_elements([(1, "hello")], ["count", "word"])
table = table1 \
    .where("LIKE(word, 'F%')") \
    .union_all(table2)

explanation = t_env.explain(table)
print(explanation)
{% endhighlight %}
</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[1], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[2], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[1], fields=[count, word])
  DataStreamScan(id=[2], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

Stage 2 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 3 : Operator
		content : from: (count, word)
		ship_strategy : REBALANCE
	
		Stage 4 : Operator
			content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
			ship_strategy : FORWARD
	
			Stage 5 : Operator
				content : from: (count, word)
				ship_strategy : REBALANCE
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[1], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[2], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[1], fields=[count, word])
  DataStreamScan(id=[2], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

Stage 2 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 3 : Operator
		content : from: (count, word)
		ship_strategy : REBALANCE
	
		Stage 4 : Operator
			content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
			ship_strategy : FORWARD
	
			Stage 5 : Operator
				content : from: (count, word)
				ship_strategy : REBALANCE
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[3], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[6], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[3], fields=[count, word])
  DataStreamScan(id=[6], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 2 : Operator
		content : Flat Map
		ship_strategy : FORWARD
	
		Stage 3 : Operator
			content : Map
			ship_strategy : FORWARD

Stage 4 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 5 : Operator
		content : Flat Map
		ship_strategy : FORWARD
	
		Stage 6 : Operator
			content : Map
			ship_strategy : FORWARD
	
			Stage 7 : Operator
				content : Map
				ship_strategy : FORWARD
	
				Stage 8 : Operator
					content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
					ship_strategy : FORWARD
	
					Stage 9 : Operator
						content : Map
						ship_strategy : FORWARD
{% endhighlight %}
</div>
</div>

下面的代码显示了一个示例以及使用`explain()`的多接收器计划的相应输出：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);

String[] fieldNames = { "count", "word" };
TypeInformation[] fieldTypes = { Types.INT, Types.STRING };
tEnv.registerTableSource("MySource1", new CsvTableSource("/source/path1", fieldNames, fieldTypes));
tEnv.registerTableSource("MySource2", new CsvTableSource("/source/path2", fieldNames, fieldTypes));
tEnv.registerTableSink("MySink1", new CsvTableSink("/sink/path1").configure(fieldNames, fieldTypes));
tEnv.registerTableSink("MySink2", new CsvTableSink("/sink/path2").configure(fieldNames, fieldTypes));

Table table1 = tEnv.scan("MySource1").where("LIKE(word, 'F%')");
table1.insertInto("MySink1");

Table table2 = table1.unionAll(tEnv.scan("MySource2"));
table2.insertInto("MySink2");

String explanation = tEnv.explain(false);
System.out.println(explanation);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
val tEnv = TableEnvironment.create(settings)

val fieldNames = Array("count", "word")
val fieldTypes = Array[TypeInformation[_]](Types.INT, Types.STRING)
tEnv.registerTableSource("MySource1", new CsvTableSource("/source/path1", fieldNames, fieldTypes))
tEnv.registerTableSource("MySource2", new CsvTableSource("/source/path2",fieldNames, fieldTypes))
tEnv.registerTableSink("MySink1", new CsvTableSink("/sink/path1").configure(fieldNames, fieldTypes))
tEnv.registerTableSink("MySink2", new CsvTableSink("/sink/path2").configure(fieldNames, fieldTypes))

val table1 = tEnv.scan("MySource1").where("LIKE(word, 'F%')")
table1.insertInto("MySink1")

val table2 = table1.unionAll(tEnv.scan("MySource2"))
table2.insertInto("MySink2")

val explanation = tEnv.explain(false)
println(explanation)

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
t_env = TableEnvironment.create(environment_settings=settings)

field_names = ["count", "word"]
field_types = [DataTypes.INT(), DataTypes.STRING()]
t_env.register_table_source("MySource1", CsvTableSource("/source/path1", field_names, field_types))
t_env.register_table_source("MySource2", CsvTableSource("/source/path2", field_names, field_types))
t_env.register_table_sink("MySink1", CsvTableSink("/sink/path1", field_names, field_types))
t_env.register_table_sink("MySink2", CsvTableSink("/sink/path2", field_names, field_types))

table1 = t_env.scan("MySource1").where("LIKE(word, 'F%')")
table1.insert_into("MySink1")

table2 = table1.union_all(t_env.scan("MySource2"))
table2.insert_into("MySink2")

explanation = t_env.explain()
print(explanation)
{% endhighlight %}
</div>
</div>

多接收器计划的结果是
<div>
{% highlight text %}

== Abstract Syntax Tree ==
LogicalSink(name=[MySink1], fields=[count, word])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])

LogicalSink(name=[MySink2], fields=[count, word])
+- LogicalUnion(all=[true])
   :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]])

== Optimized Logical Plan ==
Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')], reuse_id=[1])
+- TableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

Sink(name=[MySink1], fields=[count, word])
+- Reused(reference_id=[1])

Sink(name=[MySink2], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Reused(reference_id=[1])
   +- TableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 2 : Operator
		content : CsvTableSource(read fields: count, word)
		ship_strategy : REBALANCE
	
		Stage 3 : Operator
			content : SourceConversion(table:Buffer(default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]), fields:(count, word))
			ship_strategy : FORWARD
	
			Stage 4 : Operator
				content : Calc(where: (word LIKE _UTF-16LE'F%'), select: (count, word))
				ship_strategy : FORWARD
	
				Stage 5 : Operator
					content : SinkConversionToRow
					ship_strategy : FORWARD
	
					Stage 6 : Operator
						content : Map
						ship_strategy : FORWARD

Stage 8 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 9 : Operator
		content : CsvTableSource(read fields: count, word)
		ship_strategy : REBALANCE
	
		Stage 10 : Operator
			content : SourceConversion(table:Buffer(default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]), fields:(count, word))
			ship_strategy : FORWARD
	
			Stage 12 : Operator
				content : SinkConversionToRow
				ship_strategy : FORWARD
	
				Stage 13 : Operator
					content : Map
					ship_strategy : FORWARD
	
					Stage 7 : Data Sink
						content : Sink: CsvTableSink(count, word)
						ship_strategy : FORWARD
	
						Stage 14 : Data Sink
							content : Sink: CsvTableSink(count, word)
							ship_strategy : FORWARD

{% endhighlight %}
</div>

{% top %}


