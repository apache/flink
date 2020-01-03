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

Table API 和 SQL 集成在同一套 API 中。该 API 的中心概念是`表`（Table），用作查询的输入和输出。本文介绍了 Table API 和 SQL 查询程序的通用结构、如何注册`表`、如何查询`表`以及如何发出`表`。

* This will be replaced by the TOC
{:toc}

两种计划器（Planner）的主要区别
-----------------------------------------

1. Blink 将批处理作业视作流处理的一种特例。严格来说，表和批数据之间的转换并不受支持，并且批处理作业也不会转换成`批处理`程序而是转换成流处理程序，`流处理`作业也一样。
2. Blink 计划器不支持  `BatchTableSource`，而是使用有界的  `StreamTableSource` 来替代。
3. Blink 计划器仅支持全新的 `Catalog` 并且不支持被弃用的 `ExternalCatalog`。
4. 原版计划器和 Blink 计划器中 `FilterableTableSource` 的实现是不兼容的。原版计划器会将 `PlannerExpression` 下推至 `FilterableTableSource`，而 Blink 计划器则是将 `Expression` 下推。
5. 基于字符串的键值配置选项仅在 Blink 计划器中使用。（详情参见 [配置]({{ site.baseurl }}/zh/dev/table/config.html) ）
6. `PlannerConfig` 在两种计划器中的实现（`CalciteConfig`）是不同的。
7. Blink 计划器会将多sink（multiple-sinks）优化成一张有向无环图（DAG）（仅支持 `TableEnvironment`，不支持 `StreamTableEnvironment`）。原版计划器总是将每个sink都优化成一个新的有向无环图，且所有图相互独立。
8. 原版计划器目前不支持 catalog 统计，而 Blink 支持。


Table API 和 SQL 程序的结构
---------------------------------------

所有用于批处理和流处理的 Table API 和 SQL 程序都遵循相同的模式。下面的代码示例展示了 Table API 和 SQL 程序的通用结构。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// 为指定计划器的批处理或流处理作业创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 注册表
tableEnv.registerTable("table1", ...)            // 或者
tableEnv.registerTableSource("table2", ...);
// 注册输出表
tableEnv.registerTableSink("outputTable", ...);

// 根据 Table API 查询结果创建表
Table tapiResult = tableEnv.scan("table1").select(...);
// 根据 SQL 查询结果创建表
Table sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");

// 发送 Table API 的结果表至 TableSink，SQL 的情形也相同
tapiResult.insertInto("outputTable");

// 执行
tableEnv.execute("java_job");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

// 为指定计划器的批处理或流处理作业创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 注册表
tableEnv.registerTable("table1", ...)           // 或者
tableEnv.registerTableSource("table2", ...)
// 注册输出表
tableEnv.registerTableSink("outputTable", ...);

// 根据 Table API 查询结果创建表
val tapiResult = tableEnv.scan("table1").select(...)
// 根据 SQL 查询结果创建表
val sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ...")

// 发送 Table API 的结果表至 TableSink，SQL 的情形也相同
tapiResult.insertInto("outputTable")

// 执行
tableEnv.execute("scala_job")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

# 为指定计划器的批处理或流处理作业创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 注册表
table_env.register_table("table1", ...)           # 或者
table_env.register_table_source("table2", ...)

# 注册输出表
table_env.register_table_sink("outputTable", ...);

# 根据 Table API 查询结果创建表
tapi_result = table_env.scan("table1").select(...)
# 根据 SQL 查询结果创建表
sql_result  = table_env.sql_query("SELECT ... FROM table2 ...")

# 发送 Table API 的结果表至 TableSink，SQL 的情形也相同
tapi_result.insert_into("outputTable")

# 执行
table_env.execute("python_job")

{% endhighlight %}
</div>
</div>

**注释：** Table API 和 SQL 查询可以很容易地集成并嵌入到流处理或批处理程序中。 请参阅[与 DataStream 和 DataSet API 结合](#integration-with-datastream-and-dataset-api) 章节了解如何将数据流和数据集与表之间的相互转化。

{% top %}

创建 TableEnvironment
-------------------------

`TableEnvironment` 是 Table API 和 SQL 的核心概念。它负责:

* 在内部的 catalog 中注册`表`
* 注册外部的 catalog
* 执行 SQL 查询
* 注册自定义函数 （scalar、table 或 aggregation）
* 将`流数据集`或`批数据集`转换成`表`
* 引用  `ExecutionEnvironment` 或 `StreamExecutionEnvironment`

`表`总是绑定在确定的 `TableEnvironment` 上。不能在同一条查询中使用不同 TableEnvironment 中的表，例如，对它们进行 join 或 union 操作。

`TableEnvironment` 可以通过静态方法 `BatchTableEnvironment.create()` 或者 `StreamTableEnvironment.create()` 在 `StreamExecutionEnvironment` 或者 `ExecutionEnvironment` 中创建，`TableConfig` 是可选项。`TableConfig`可用于配置`TableEnvironment`或自定义查询优化和转换过程(参见 [查询优化](#query-optimization))。

请确保选择与你的编程语言匹配的确定的计划器`BatchTableEnvironment`/`StreamTableEnvironment`。

如果两种计划器的 jar 包都在 classpath 中（默认行为），你应该明确地设置要在当前程序中使用的计划器。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// **********************
// FLINK 流式查询
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
// 或者 TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);

// ******************
// FLINK 批查询
// ******************
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

// **********************
// BLINK 流式查询
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
// 或者 TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

// ******************
// BLINK 批查询
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
// FLINK 流式查询
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
// 或者 val fsTableEnv = TableEnvironment.create(fsSettings)

// ******************
// FLINK 批查询
// ******************
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

val fbEnv = ExecutionEnvironment.getExecutionEnvironment
val fbTableEnv = BatchTableEnvironment.create(fbEnv)

// **********************
// BLINK 流式查询
// **********************
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
// 或者 val bsTableEnv = TableEnvironment.create(bsSettings)

// ******************
// BLINK 批查询
// ******************
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
val bbTableEnv = TableEnvironment.create(bbSettings)

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

# **********************
# FLINK 流式查询
# **********************
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

f_s_env = StreamExecutionEnvironment.get_execution_environment()
f_s_settings = EnvironmentSettings.new_instance().use_old_planner().in_streaming_mode().build()
f_s_t_env = StreamTableEnvironment.create(f_s_env, environment_settings=f_s_settings)

# ******************
# FLINK 批查询
# ******************
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment

f_b_env = ExecutionEnvironment.get_execution_environment()
f_b_t_env = BatchTableEnvironment.create(f_b_env, table_config)

# **********************
# BLINK 流式查询
# **********************
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

b_s_env = StreamExecutionEnvironment.get_execution_environment()
b_s_settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
b_s_t_env = StreamTableEnvironment.create(b_s_env, environment_settings=b_s_settings)

# ******************
# BLINK 批查询
# ******************
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

b_b_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
b_b_t_env = BatchTableEnvironment.create(environment_settings=b_b_settings)

{% endhighlight %}
</div>
</div>

**注释：** 如果`/lib`目录中只有一中计划器的 jar 包，则可以使用`useAnyPlanner`（python 使用`use any'u planner`）创建`EnvironmentSettings`。


{% top %}

在 Catalog 中注册表
-------------------------------

`TableEnvironment` 维护着一张按名称注册的表的 Catalog。有两种类型的表，*输入表* 和*输出表*。输入表可以被 Table API 和 SQL 查询应用并提供输入数据。输出表可以用于向外部系统发出 Table API 和 SQL 查询的查询结果。

有多种数据源可以被注册为输入表：

* 一个已经存在的 `Table` 对象，一般是Table API 和 SQL 查询的查询结果。
* `TableSource`，用于访问外部数据，例如文件、数据库或消息系统。
*  由流处理（仅限流处理作业） 或者批处理 （仅限由原版计划器转换的批处理作业）程序得到的`DataStream` 或者 `DataSet`。 `DataStream` 或者 `DataSet` 的注册方法在[与 DataStream 和 DataSet API 结合](#integration-with-datastream-and-dataset-api) 章节中讨论。

输出表可以通过 `TableSink` 注册。

### 注册表

按下述流程可以将`表`注册到`TableEnvironment`中：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 表是简单投影查询的结果
Table projTable = tableEnv.scan("X").select(...);

// 将表projTable注册为表“ projectedTable”
tableEnv.registerTable("projectedTable", projTable);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 表是简单投影查询的结果
val projTable: Table = tableEnv.scan("X").select(...)

// 将表projTable注册为表“ projectedTable”
tableEnv.registerTable("projectedTable", projTable)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 表是简单投影查询的结果
proj_table = table_env.scan("X").select(...)

# 将表projTable注册为表“ projectedTable”
table_env.register_table("projectedTable", proj_table)
{% endhighlight %}
</div>
</div>

**注释：** 已注册的`表`的处理方式与关系数据库系统中已知的`视图（view）`类似，即，定义`表`的查询未经过优化，但在另一个查询关联已注册的`表`时将内联（inline）。如果多个查询关联同一个已注册的`表`，则将为每个关联查询内联该查询并执行多次，即，已注册的`表`的结果将不会共享。

{% top %}

### 注册 TableSource

`TableSource`可访问存储在存储系统中的外部数据，例如数据库（MySQL，HBase 等），具有特定编码的文件（CSV，Apache \[Parquet，Avro，ORC \]， ...）或消息传递系统（Apache Kafka，RabbitMQ 等）。

Flink旨在为常见的数据格式和存储系统提供 TableSources。请参阅 [Table Sources 和 Sinks]({{ site.baseurl }}/zh/dev/table/sourceSinks.html) 获取有关受支持的 TableSources 的列表以及如何构建自定义 `TableSource` 的说明。

在 `TableEnvironment` 中注册 `TableSource` 的过程如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节
// 创建 TableSource
TableSource csvSource = new CsvTableSource("/path/to/file", ...);

// 将 TableSource 注册为表“ CsvTable”
tableEnv.registerTableSource("CsvTable", csvSource);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 创建 TableSource
val csvSource: TableSource = new CsvTableSource("/path/to/file", ...)

// 将 TableSource 注册为表“ CsvTable”
tableEnv.registerTableSource("CsvTable", csvSource)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 创建 TableSource
csv_source = CsvTableSource("/path/to/file", ...)

# 将 TableSource 注册为表“ CsvTable”
table_env.register_table_source("csvTable", csv_source)
{% endhighlight %}
</div>
</div>

**注释：** 对于 Blink 计划器，“TableEnvironment”只接受“StreamTableSource”、“LookupableTableSource”和“InputFormatTableSource”，用于批处理的“StreamTableSource”必须有界。

{% top %}

### 注册 Table Sink

注册的 `TableSink` 可以被用来向外部存储系统，例如数据库、键值存储、消息队列，或文件系统（以不同的编码方式，例如CSV、Apache \[Parquet、Avro、ORC\]，…）[发出 Table API 或 SQL 查询的结果](common.html#emit-a-table)。

Flink旨在为常见的数据格式和存储系统提供 TableSink。请参阅文档 [Table Sources 和 Sinks]({{ site.baseurl }}/zh/dev/table/sourceSinks.html)  获取关于可用 sink 的详细信息以及如何实现自定义 `TableSink` 的说明。

在 `TableEnvironment` 中注册 `TableSink` 的过程如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 创建 TableSink
TableSink csvSink = new CsvTableSink("/path/to/file", ...);

// 定义字段名称和类型
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};

// 将 TableSink 注册为表“CsvSinkTable”
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 创建 TableSink
val csvSink: TableSink = new CsvTableSink("/path/to/file", ...)

// 定义字段名称和类型
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.LONG)

// 将 TableSink 注册为表“CsvSinkTable”
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 定义字段名称和类型
field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# 创建 TableSink
csv_sink = CsvTableSink(field_names, field_types, "/path/to/file", ...)

# 将 TableSink 注册为表“CsvSinkTable”
table_env.register_table_sink("CsvSinkTable", csv_sink)
{% endhighlight %}
</div>
</div>

{% top %}

查询表
-------------

### Table API

Table API 是关于 Scala 和 Java 的集成语言式查询 API。与 SQL 相反，Table API 的查询不是由字符串指定，而是在宿主语言中逐步构成。

Table API 是基于 `Table` 类的，该类类代表表（流或批处理），并提供使用关系操作的方法。这些方法返回一个新的 Table 对象，该对象表示对输入 Table 进行关系操作的结果。 一些关系操作由多个方法调用组成，例如 `table.groupBy(...).select()`，其中 `groupBy(...)` 指定 `table` 的分组，而 `select(...)` 在  `table` 分组上的投影。

文档 [Table API]({{ site.baseurl }}/zh/dev/table/tableApi.html) 说明了所有流处理和批处理表支持的 Table API 算子。

以下示例展示了一个简单的 Table API 聚合查询：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 注册“Orders”表

// 扫描注册的“Orders”表
Table orders = tableEnv.scan("Orders");
// 计算所有法国客户的收入
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
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 注册“Orders”表

// 扫描注册的“Orders”表
val orders = tableEnv.scan("Orders")
// 计算所有法国客户的收入
val revenue = orders
  .filter('cCountry === "FRANCE")
  .groupBy('cID, 'cName)
  .select('cID, 'cName, 'revenue.sum AS 'revSum)

// 发出或转换表
// 执行查询
{% endhighlight %}

**注释：** Scala Table API使用该符号以单个刻度（`'`）开头的 Scala 符号以引用 Table 的属性。Table API 使用 Scala 隐式转换。请确保引入了 `org.apache.flink.api.scala._` 包和 `org.apache.flink.table.api.scala._` 包以使用 Scala 隐格式转换。
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建 TableEnvironment
table_env = # 参阅“创建 TableEnvironment ”章节

# 注册“Orders”表

# 扫描注册的“Orders”表
orders = table_env.scan("Orders")
# 计算所有法国客户的收入
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

Flink SQL 是基于实现了SQL标准的 [Apache Calcite](https://Calcite.Apache.org) 的。SQL 查询由常规字符串指定。

文档 [SQL]({{ site.baseurl }}/zh/dev/table/sql.html) 描述了Flink对流处理和批处理表的SQL支持。

下面的示例演示了如何指定查询并将结果作为 `Table` 对象返回。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 注册“Orders”表

// 计算所有法国客户的收入
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
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 注册“Orders”表

// 计算所有法国客户的收入
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
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 注册“Orders”表

# 计算所有法国客户的收入
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

下面的示例演示如何通过更新查询将其结果插入已注册表。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 注册"Orders"表
// 注册"RevenueFrance"输出表

// 计算所有法国客户的收入并发送至"RevenueFrance"表
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
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 注册"Orders"表
// 注册"RevenueFrance"输出表

// 计算所有法国客户的收入并发送至"RevenueFrance"表
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
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

# 注册"Orders"表
# 注册"RevenueFrance"输出表

# 计算所有法国客户的收入 and emit to "RevenueFrance"
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

### 混用 Table API 和 SQL

Table API 和 SQL 查询的混用非常简单因为它们都返回 `Table` 对象：

* 可以在 SQL 查询返回的 `Table` 对象上定义 Table API 查询。
* 通过在 `TableEnvironment` 中注册[结果表](#register-a-table)并在 SQL 查询的 `FROM` 子句中引用它，可以在 Table API 查询的结果上定义 SQL 查询。

{% top %}

发出表
------------

`表`通过写入 `TableSink` 发出。`TableSink` 是一个通用接口，用于支持多种文件格式（如 CSV、Apache Parquet、Apache Avro）、存储系统（如 JDBC、Apache HBase、Apache Cassandra、Elasticsearch）或消息传递系统（如 Apache Kafka、RabbitMQ）。

批处理`表`只能写入 `BatchTableSink`，而流处理`表`需要指定写入 `AppendStreamTableSink`，`RetractStreamTableSink` 或者 `UpsertStreamTableSink`。

请参考文档 [Table Sources & Sinks]({{ site.baseurl }}/zh/dev/table/sourceSinks.html) 以获取更多关于可用 Sink 的信息以及如何自定义 `TableSink`。

方法 `Table.insertInto(String tableName)` 将`表`发送至已注册的 `TableSink`。该方法通过名称在 catalog 中查找 `TableSink` 并确认`Table` schema 和 `TableSink` schema 一致。

下面的示例演示如何发出 `Table`：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 TableEnvironment
TableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 创建 TableSink
TableSink sink = new CsvTableSink("/path/to/file", fieldDelim = "|");

// 注册 TableSink 并指明 schema
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);

// 通过 Table API 的算子或者 SQL 查询得出结果表
Table result = ...
// 发送结果表至已注册的 TableSink
result.insertInto("CsvSinkTable");

// 执行程序
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

// 创建 TableSink
val sink: TableSink = new CsvTableSink("/path/to/file", fieldDelim = "|")

// 注册 TableSink 并指明 schema
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation] = Array(Types.INT, Types.STRING, Types.LONG)
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink)

// 通过 Table API 的算子或者 SQL 查询得出结果表
val result: Table = ...

// 发送结果表至已注册的 TableSink
result.insertInto("CsvSinkTable")

// 执行程序
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# 创建 TableEnvironment
table_env = ... # 参阅“创建 TableEnvironment ”章节

field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# 创建 TableSink
sink = CsvTableSink(field_names, field_types, "/path/to/file", "|")

table_env.register_table_sink("CsvSinkTable", sink)

# 通过 Table API 的算子或者 SQL 查询得出结果表
result = ...

# 发送结果表至已注册的 TableSink
result.insert_into("CsvSinkTable")

# 执行程序
{% endhighlight %}
</div>
</div>

{% top %}


解析与执行查询
-----------------------------

两种计划器解析和执行查询的方式是不同的。

<div class="codetabs" markdown="1">
<div data-lang="Old planner" markdown="1">
Table API 和 SQL 查询会被解析成 [流处理]({{ site.baseurl }}/zh/dev/datastream_api.html)或者[批处理]({{ site.baseurl }}/zh/dev/batch)程序， 这取决于它们的输入数据源是流式的还是批式的。查询在内部表示为逻辑查询计划，并被解析成两个阶段：

1. 优化逻辑执行计划
2. 解析成流处理或批处理程序

Table API 或者 SQL 查询在下列情况下会被解析：

* `表`被发送给 `TableSink`，即当调用 `Table.insertInto()` 时。
* SQL 更新语句执行时，即，当调用 `TableEnvironment.sqlUpdate()` 时。
* `表`被转换成`流数据`或者`批数据`时（参阅[与 DataStream 和 DataSet API 结合](#integration-with-datastream-and-dataset-api)）。

解析完成后，Table API 或者 SQL 查询会被当做普通的流处理或批处理程序对待并且会在调用 `StreamExecutionEnvironment.execute()` 或 `ExecutionEnvironment.execute()` 的时候被执行。

</div>

<div data-lang="Blink planner" markdown="1">
Table API 和 SQL 查询会被转换成[流处理]({{ site.baseurl }}/zh/dev/datastream_api.html)程序不论它们的输入数据源是流式的还是批式的。查询在内部表示为逻辑查询计划，并被解析成两个阶段：

1. 优化逻辑执行计划
2. 解析成流处理程序

TableEnvironment 和 StreamTableEnvironment 解析查询的方式不同。

对于 `TableEnvironment`，Table API 或者 SQL 查询会在调用 `TableEnvironment.execute()` 时被解析，因为 `TableEnvironment` 会将多 sink 优化成一张有向无环图。

而对于 `StreamTableEnvironment`，当下列情况发生时，Table API 或者 SQL 查询会被解析：

* `表` 被发送至`TableSink`，即，当 `Table.insertInto()` 被调用时。
* SQL 更新语句执行时，即，当调用 `TableEnvironment.sqlUpdate()` 时。
* `表`被转换成`流数据`时。

解析完成后，Table API 或者 SQL 查询会被当做普通的流处理程序对待并且会在调用 `TableEnvironment.execute()` 或者 `StreamExecutionEnvironment.execute()` 的时候被执行。

</div>
</div>

{% top %}

与 DataStream 和 DataSet API 结合
-------------------------------------------

在流处理方面两种计划器都可以与 `DataStream` API 结合。只有原版计划器可以与 `DataSet API` 结合。在批处理方面，Blink 计划器不能同两种计划器中的任何一个结合。  
**注释：** The `DataSet` API discussed below is only relevant for the old planner on batch.

Table API 和 SQL 可以被很容易地集成并嵌入到[流处理]({{ site.baseurl }}/zh/dev/datastream_api.html)和[批处理]({{ site.baseurl }}/zh/dev/batch)程序中。例如，可以查询外部表（例如从 RDBMS），进行一些预处理，例如过滤，投影，聚合或与元数据 join，然后使用 DataStream 或 DataSet API（以及在这些 API 之上构建的任何库，例如 CEP 或 Gelly）。相反，也可以将 Table API 或 SQL 查询应用于流处理或批处理程序的结果。

这种交互可以通过`批数据集`或`流数据集`与`表`的相互转化实现。本节我们会介绍这些转化是如何实现的。

### Implicit Conversion for Scala

Scala Table API 含有对 `DataSet`、`DataStream` 和 `Table` 类的隐式转换。 通过为 Scala DataStream API 导入 `org.apache.flink.table.api.scala._` 包以及 `org.apache.flink.api.scala._` 包，可以启用这些转换。

### 将流数据集或批数据集注册成表

在 `TableEnvironment` 中可以将`批数据集或`流数据集`注册成表。结果表的 schema 取决于注册的`批数据集或`流数据集`的数据类型。请参阅文档 [数据类型到 table schema 的映射](#mapping-of-data-types-to-table-schema) for details.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment
// 在 BatchTableEnvironment 中注册批数据集的方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将流数据集注册为具有字段 “f0”，“f1” 的表 “myTable”
tableEnv.registerDataStream("myTable", stream);

// 将流数据集注册为具有字段 “myLong”，“myString” 的表 “myTable2”
tableEnv.registerDataStream("myTable2", stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
// 批数据集的注册方式相同
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[(Long, String)] = ...

// 将流数据集注册为具有字段 “f0”，“f1” 的表 “myTable”
tableEnv.registerDataStream("myTable", stream)

// 将流数据集注册为具有字段 “myLong”，“myString” 的表 “myTable2”
tableEnv.registerDataStream("myTable2", stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

**注释：** `流数据集`和`表`的名称不能使用 `^_DataStreamTable_[0-9]+` 的格式，而`批数据集`和`表`的名称不能使用 `^_DataSetTable_[0-9]+` 的格式。这些格式仅保留做内部使用。

{% top %}

### 将批数据集或流数据集转换成表

除了在 `TableEnvironment` 中注册`流数据集`和`批数据集`的方法外，也可以将它们直接转换成`表`。如果你要在 Table API 查询中使用`表`，这将很方便。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment
// 在 BatchTableEnvironment 中注册批数据集的方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将流数据集转换成具有默认字段 “f0”, “f1” 的表
Table table1 = tableEnv.fromDataStream(stream);

// 将流数据集转换成具有字段 “myLong”, “myString” 的表
Table table2 = tableEnv.fromDataStream(stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
// 批数据集的注册方式相同
val tableEnv = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[(Long, String)] = ...

// 将流数据集转换成具有默认字段 '_1, '_2 的表
val table1: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成具有字段 'myLong, 'myString 的表
val table2: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

{% top %}

### 将表转换成流数据集或批数据集

`表`可以被转换成`流数据集`或`批数据集`。通过这种方式，自定义的批处理或流处理程序就可以在 Table API 或者 SQL 的查询结果上运行了。

将`表`转换为`流数据集`或者`批数据集`时，你需要指定生成的`流数据集`或`批数据集`的数据类型，即，`表`的每行数据要转换成的数据类型。通常最方便的选择是转换成 `Row` 。以下列表概述了不同选项的功能：

- **Row**: 字段按位置映射，字段数量任意，支持 `null` 值，无类型安全（type-safe）检查。
- **POJO**: 字段按名称映射（POJO 必须按`表`中字段名称命名），字段数量任意，支持 `null` 值，无类型安全检查。
- **Case Class**: 字段按位置映射，不支持 `null` 值，有类型安全检查。
- **Tuple**: 字段按位置映射，字段数量少于 22（Scala）或者 25（Java），不支持 `null` 值，无类型安全检查。
- **Atomic Type**: `表`必须有一个字段，不支持 `null` 值，有类型安全检查。

#### 将表转换成流数据集

流式查询（streaming query）的结果表会动态更新，即，当新纪录到达查询的输入流时，查询结果会改变。因此，像这样将动态查询结果转换成流`数据集`需要对表的更新方式进行编码。

将`表`转换为`流数据集`有两种模式：

1. **Append Mode**: 仅当动态`表`仅通过`INSERT`更改进行修改时，才可以使用此模式，即，它仅是追加操作，并且之前发出的结果永远不会更新。
2. **Retract Mode**: 任何情形都可以使用此模式。它使用 boolean 值对 `INSERT` 和 `DELETE` 操作的数据进行标记。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment.
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 有两个字段的表（String name, Integer age）
Table table = ...

// 通过指定类将表转换为 Row 的追加（append）流数据集
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

// 通过 TypeInformation 将表转换为 Tuple2 <String，Integer> 的追加流数据集
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple =
  tableEnv.toAppendStream(table, tupleType);

// 将表转换成 Row 的回收（retract）流数据集
//   类型X的回收流就是 DataStream<Tuple2<Boolean, X>>。
//   boolean 字段表示更改的类型。
//   True 表示 INSERT，false 表示 DELETE
DataStream<Tuple2<Boolean, Row>> retractStream =
  tableEnv.toRetractStream(table, Row.class);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment.
// 批数据集的注册方式相同
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

// 有两个字段的表（String name, Integer age）
val table: Table = ...

// 将表转换成 Row 的追加流数据集
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// 将表转换成 Tuple2[String, Int] 的追加流数据集
val dsTuple: DataStream[(String, Int)] dsTuple =
  tableEnv.toAppendStream[(String, Int)](table)

// 将表转换成 Row 的回收（retract）流数据集
//   类型X的回收流就是 DataStream[(Boolean, X)]
//   boolean 字段表示更改的类型。
//   True 表示 INSERT，false 表示 DELETE
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
{% endhighlight %}
</div>
</div>

**注释：** 文档[动态表](streaming/dynamic_tables.html)给出了有关动态表及其属性的详细讨论。

#### 将表转换成批数据集

将`表`转换成`批数据集`的过程如下：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 BatchTableEnvironment
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

// 有两个字段的表（String name, Integer age）
Table table = ...

// 通过指定类将表转换成 Row 的批数据集
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

// 通过 TypeInformation 将表转换为 Tuple2<String，Integer> 的批数据集
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple =
  tableEnv.toDataSet(table, tupleType);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
// 批数据集的注册方式相同
val tableEnv = BatchTableEnvironment.create(env)

// 有两个字段的表（String name, Integer age）
val table: Table = ...

// 将表转换成 Row 的批数据集
val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](table)

// 将表转换成 Tuple2[String, Int] 的批数据集
val dsTuple: DataSet[(String, Int)] = tableEnv.toDataSet[(String, Int)](table)
{% endhighlight %}
</div>
</div>

{% top %}

### 数据类型到 Table Schema 的映射

Flink 的 DataStream 和 DataSet APIs 支持多样的数据类型。例如 Tuple（Scala 内置以及Flink Java tuple）、POJO 类型、Scala case class 类型以及 Flink 的 Row 类型等允许嵌套且有多个可在表的表达式中访问的字段的复合数据类型。其他类型被视为原子类型。 Composite types such as Tuples (built-in Scala and Flink Java tuples), POJOs, Scala case classes, and Flink's Row type allow for nested data structures with multiple fields that can be accessed in table expressions. 下面，我们讨论 Table API 如何将这些数据类型类型转换为内部 row 表示形式，并提供将流数据集转换成表的样例。

数据类型到 table schema 的映射有两种方式：**基于字段位置**或**基于字段名称**。

**基于位置映射**

基于位置的映射可在保持字段顺序的同时为字段提供更有意义的名称。这种映射方式可用于*具有确定的字段顺序*的复合数据类型以及原子类型。如 tuple、row 以及 case class 这些复合数据类型都有这样的字段顺序。然而，POJO 类型的字段则必须通过名称映射（参见下一章）。可以将字段投影出来，但不能使用`as`重命名。

定义基于位置的映射时，输入数据类型中一定不能存在指定的名称，否则 API 会假定应该基于字段名称进行映射。如果未指定任何字段名称，则使用默认的字段名称和复合数据类型的字段顺序，或者使用 “ f0” 表示原子类型。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节;

DataStream<Tuple2<Long, Integer>> stream = ...

// 将流数据集转换成有默认字段 “f0” 和 “f1” 的表
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成只有字段 “myLong” 的表
Table table = tableEnv.fromDataStream(stream, “myLong”);

// 将流数据集转换成有字段 “myLong” 和 “myInt” 的表
Table table = tableEnv.fromDataStream(stream, "myLong, myInt");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[(Long, Int)] = ...

// 将流数据集转换成有默认字段 “_1” 和 “_2” 的表
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成只有字段 “myLong” 的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong)

// 将流数据集转换成有字段 “myLong” 和 “myInt” 的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myInt)
{% endhighlight %}
</div>
</div>

**基于名称的映射**

基于名称的映射适用于任何数据类型包括 POJO 类型。这是定义 table schema 映射最灵活的方式。映射中的所有字段均按名称引用，并且可以通过 `as` 重命名。字段可以被重新排序和映射。

若果没有指定任何字段名称，则使用默认的字段名称和复合数据类型的字段顺序，或者使用 `f0` 表示原子类型。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

DataStream<Tuple2<Long, Integer>> stream = ...

// 将流数据集转换成有默认字段 “f0” 和 “f1” 的表
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成只有字段 “f1” 的表
Table table = tableEnv.fromDataStream(stream, “f1”);

// 将流数据集转换成表并交换字段顺序
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// 将流数据集转换成表并交换字段顺序，命名为 “myInt” 和 “myLong”
Table table = tableEnv.fromDataStream(stream, "f1 as myInt, f0 as myLong");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[(Long, Int)] = ...

// 将流数据集转换成有默认字段 “_1” 和 “_2” 的表
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成仅有字段 “_2” 的表
val table: Table = tableEnv.fromDataStream(stream, '_2)

// 将流数据集转换成表并交换字段顺序
val table: Table = tableEnv.fromDataStream(stream, '_2, '_1)

// 将流数据集转换成表并交换字段顺序，将字段命名为 “myInt” 和 “myLong”
val table: Table = tableEnv.fromDataStream(stream, '_2 as 'myInt, '_1 as 'myLong)
{% endhighlight %}
</div>
</div>

#### 原子类型

Flink 将原始数据类型（`Integer`、`Double`、`String`）或者通用数据类型（不可再拆分的数据类型）视为原子类型。原子类型的`流数据集`或者`批数据集`会被转换成只有一条属性的`表`。属性的数据类型可以由原子类型推断出，还可以重新命名属性。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

DataStream<Long> stream = ...

// 将流数据集转换成有默认字段 “f0” 的表
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成有字段 “myLong” 的表
Table table = tableEnv.fromDataStream(stream, “myLong”);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[Long] = ...

// 将流数据集转换成有默认字段 “f0” 的表
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成有字段 “myLong” 的表
val table: Table = tableEnv.fromDataStream(stream, 'myLong)
{% endhighlight %}
</div>
</div>

#### Tuple类型（Scala 和 Java）和 Case Class类型（仅 Scala）

Flink 支持 Scala 的内置 tuple 类型并给 Java 提供自己的 tuple 类型。两种 tuple 的流数据集和批数据集都能被转换成表。可以通过提供所有字段名称来重命名字段（基于位置映射）。如果没有指明任何字段名称，则会使用默认的字段名称。如果引用了原始字段名称（对于 Flink tuple 为`f0`、`f1` ... ...，对于 Scala tuple 为`_1`、`_2` ... ...），则 API 会假定映射是基于名称的而不是基于位置的。基于名称的映射可以通过 `as` 对字段和投影进行重新排序。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

DataStream<Tuple2<Long, String>> stream = ...

// 将流数据集转换成有默认字段字段“f0”、“f1”的表
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成表并重命名字段为“myLong”、“myString”（基于位置）
Table table = tableEnv.fromDataStream(stream, "myLong, myString");

// 将流数据集转换成表并将字段重新排序为“f1”、“f1”（基于名称）
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// 将流数据集转换成有投影字段“f1”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, “f1”);

// 将流数据集转换成表并重新命名排序为“myString”、“myLong”（基于名称）
Table table = tableEnv.fromDataStream(stream, "f1 as 'myString', f0 as 'myLong'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

val stream: DataStream[(Long, String)] = ...

// 将流数据集转换成有重命名默认字段 '_1、'_2 的表
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成有字段“myLong”、“myString”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)

// 将流数据集转换成有重排序字段“_2”、“_1”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, '_2, '_1)

// 将流数据集转换成有映射字段“_2”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, '_2)

// 将流数据集转换成表并重新命名排序为“myString”、“myLong”（基于名称）
val table: Table = tableEnv.fromDataStream(stream, '_2 as 'myString, '_1 as 'myLong)

// 定义 case class 类型
case class Person(name: String, age: Int)
val streamCC: DataStream[Person] = ...

// 将流数据集转换成有默认字段 'name、'age 的表
val table = tableEnv.fromDataStream(streamCC)

// 将流数据集转换成有字段 'myName、'myAge 的表（基于位置）
val table = tableEnv.fromDataStream(streamCC, 'myName, 'myAge)

// 将流数据集转换成有重排序字段“myAge”、“myName”的表（基于名称））
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

{% endhighlight %}
</div>
</div>

#### POJO 类型 （Java 和 Scala）

Flink 支持 POJO 类型作为复合类型。确定 POJO 类型的规则记录在[这里]({{ site.baseurl }}/zh/dev/api_concepts.html#pojos).

在不指定字段名称的情况下将 POJO 类型的`流数据集`或`批数据集`转换成`表`时，将使用原始 POJO 类型字段的名称。名称映射需要原始名称，并且不能按位置进行。字段可以使用别名（带有 `as` 关键字）来重命名，重新排序和投影。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// Person类时带有“name”和“age”字段的POJO类型
DataStream<Person> stream = ...

// 将流数据集转换成带有默认字段“age”和“name”的表（字段根据名称排序）
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成带有重命名字段“myAge”和“myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "age as myAge, name as myName");

// 将流数据集转换成带有映射字段“name”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name");

// 将流数据集转换成带有重命名的映射字段“myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

// Person类是拥有字段“name”和“age”的 POJO 类型
val stream: DataStream[Person] = ...

// 将流数据集转换成带有默认字段“age”和“name”的表（字段根据名称排序）
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成带有重命名字段“myAge”和“myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

// 将流数据集转换成带有映射字段“name”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name)

// 将流数据集转换成带有重命名的映射字段“myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

#### Row类型

`Row` 类型支持任意数量的字段以及具有 `null` 值的字段。字段名称可以通过 `RowTypeInfo` 指定，也可以在将 `Row` 的`流数据集`或`批数据集`转换为`表`时指定。Row 类型的字段映射支持基于名称和基于位置两种方式。字段可以通过提供所有字段的名称的方式重命名（基于位置映射）或者分别选择进行投影/排序/重命名（基于名称映射）。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 创建 StreamTableEnvironment，创建 BatchTableEnvironment 方法相同
StreamTableEnvironment tableEnv = ...; // 参阅“创建 TableEnvironment ”章节

// 包含在两个字段“name”和“age”的 Row 的数据流，在 `RowTypeInfo` 中指明字段名称
DataStream<Row> stream = ...

// 将流数据集转换成含有默认字段“name”，“age”的表
Table table = tableEnv.fromDataStream(stream);

// 将流数据集转换成含有重命名字段“myName”，“myAge”的表（基于位置）
Table table = tableEnv.fromDataStream(stream, "myName, myAge");

// 将流数据集转换成含有重命名字段“myName”，“myAge”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName, age as myAge");

// 将流数据集转换成带有映射字段“name”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name");

// 将流数据集转换成带有重命名的映射字段“myName”的表（基于名称）
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// 创建 TableEnvironment
val tableEnv: StreamTableEnvironment = ... // 参阅“创建 TableEnvironment ”章节

// 包含在两个字段“name”和“age”的 Row 的数据流，在 `RowTypeInfo` 中指明字段名称
val stream: DataStream[Row] = ...

// 将流数据集转换成含有默认字段“name”，“age”的表
val table: Table = tableEnv.fromDataStream(stream)

// 将流数据集转换成含有重命名字段“myName”，“myAge”的表（基于位置）
val table: Table = tableEnv.fromDataStream(stream, 'myName, 'myAge)

// 将流数据集转换成含有重命名字段“myName”，“myAge”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName, 'age as 'myAge)

// 将流数据集转换成带有映射字段“name”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name)

// 将流数据集转换成带有重命名的映射字段“myName”的表（基于名称）
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

{% top %}


查询优化
------------------

<div class="codetabs" markdown="1">
<div data-lang="Old planner" markdown="1">

Apache Flink 利用 Apache Calcite 来优化和解析查询。当前执行的优化包括投影和过滤器下推，子查询去相关以及其他类型的查询重写。原版计划程序尚未优化 join 的顺序，而是按照查询中定义的顺序执行它们（FROM 子句中的表顺序和/或 WHERE 子句中的 join 谓词顺序）。

通过提供一个 `CalciteConfig` 对象，可以调整在不同阶段应用的优化规则集合。这个对象可以通过调用构造器 `CalciteConfig.createBuilder()` 创建，并通过调用 `tableEnv.getConfig.setPlannerConfig(calciteConfig)` 提供给 TableEnvironment。

</div>

<div data-lang="Blink planner" markdown="1">

Apache Flink 使用并扩展了 Apache Calcite 来执行复杂的查询优化。
这包括一系列基于规则和成本的优化，例如：

* 基于 Apache Calcite 的子查询解相关
* 模型剪枝
* 分区剪枝
* 过滤器下推
* 子计划消除重复数据以避免重复计算
* 特殊子查询重写，包括两部分：
    * 将 IN 和 EXISTS 转换为 left semi-joins
    * 将 NOT IN 和 NOT EXISTS 转换为 left anti-join
* 可选 join 重新排序
    * 通过 `table.optimizer.join-reorder-enabled` 启用

**注释：** 当前仅在子查询重写的结合条件下支持 IN / EXISTS / NOT IN / NOT EXISTS。

优化器不仅基于计划，而且还基于可从数据源获得的丰富统计信息以及每个算子（例如 io，cpu，网络和内存）的细粒度成本来做出明智的决策。

高级用户可以通过 `CalciteConfig` 对象提供自定义优化，可以通过调用  `TableEnvironment＃getConfig＃setPlannerConfig` 将其提供给 TableEnvironment。

</div>
</div>


### 解释表

Table API 提供了一种机制来解释计算`表`的逻辑和优化查询计划。
这是通过 `TableEnvironment.explain(table)` 或者 `TableEnvironment.explain()` 完成的。`explain(table)` 返回给定`表`的计划。 `explain()` 返回多 sink 计划的结果并且主要用于 Blink 计划器。它返回一个描述三中计划的字符串：

1. 关系查询的抽象语法树（the Abstract Syntax Tree），即未优化的逻辑查询计划，
2. 优化的逻辑查询计划，以及
3. 物理执行计划。

以下代码展示了一个示例以及对给定`表`使用 `explain（table）` 的相应输出：

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

	Stage 3 ： Operator
		content ： from： (count, word)
		ship_strategy ： REBALANCE

		Stage 4 ： Operator
			content ： where： (LIKE(word, _UTF-16LE'F%')), select： (count, word)
			ship_strategy ： FORWARD

			Stage 5 ： Operator
				content ： from： (count, word)
				ship_strategy ： REBALANCE
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

	Stage 3 ： Operator
		content ： from： (count, word)
		ship_strategy ： REBALANCE

		Stage 4 ： Operator
			content ： where： (LIKE(word, _UTF-16LE'F%')), select： (count, word)
			ship_strategy ： FORWARD

			Stage 5 ： Operator
				content ： from： (count, word)
				ship_strategy ： REBALANCE
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

	Stage 2 ： Operator
		content ： Flat Map
		ship_strategy ： FORWARD

		Stage 3 ： Operator
			content ： Map
			ship_strategy ： FORWARD

Stage 4 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 5 ： Operator
		content ： Flat Map
		ship_strategy ： FORWARD

		Stage 6 ： Operator
			content ： Map
			ship_strategy ： FORWARD

			Stage 7 ： Operator
				content ： Map
				ship_strategy ： FORWARD

				Stage 8 ： Operator
					content ： where： (LIKE(word, _UTF-16LE'F%')), select： (count, word)
					ship_strategy ： FORWARD

					Stage 9 ： Operator
						content ： Map
						ship_strategy ： FORWARD
{% endhighlight %}
</div>
</div>

以下代码展示了一个示例以及使用 `explain（）` 的多 sink 计划的相应输出：

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

多 sink 计划的结果是：
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

	Stage 2 ： Operator
		content ： CsvTableSource(read fields： count, word)
		ship_strategy ： REBALANCE

		Stage 3 ： Operator
			content ： SourceConversion(table：Buffer(default_catalog, default_database, MySource1, source： [CsvTableSource(read fields： count, word)]), fields：(count, word))
			ship_strategy ： FORWARD

			Stage 4 ： Operator
				content ： Calc(where： (word LIKE _UTF-16LE'F%'), select： (count, word))
				ship_strategy ： FORWARD

				Stage 5 ： Operator
					content ： SinkConversionToRow
					ship_strategy ： FORWARD

					Stage 6 ： Operator
						content ： Map
						ship_strategy ： FORWARD

Stage 8 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 9 ： Operator
		content ： CsvTableSource(read fields： count, word)
		ship_strategy ： REBALANCE

		Stage 10 ： Operator
			content ： SourceConversion(table：Buffer(default_catalog, default_database, MySource2, source： [CsvTableSource(read fields： count, word)]), fields：(count, word))
			ship_strategy ： FORWARD

			Stage 12 ： Operator
				content ： SinkConversionToRow
				ship_strategy ： FORWARD

				Stage 13 ： Operator
					content ： Map
					ship_strategy ： FORWARD

					Stage 7 ： Data Sink
						content ： Sink： CsvTableSink(count, word)
						ship_strategy ： FORWARD

						Stage 14 ： Data Sink
							content ： Sink： CsvTableSink(count, word)
							ship_strategy ： FORWARD

{% endhighlight %}
</div>

{% top %}
