---
title: "Table API"
weight: 31
type: docs
aliases:
  - /zh/dev/table/tableApi.html
  - /zh/dev/table_api.html
  - /zh/apis/table.html
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

# Table API

Table API 是批处理和流处理的统一的关系型 API。Table API 的查询不需要修改代码就可以采用批输入或流输入来运行。Table API 是 SQL 语言的超集，并且是针对 Apache Flink 专门设计的。Table API 集成了 Scala，Java 和 Python 语言的 API。Table API 的查询是使用  Java，Scala 或 Python 语言嵌入的风格定义的，有诸如自动补全和语法校验的 IDE 支持，而不是像普通 SQL 一样使用字符串类型的值来指定查询。

Table API 和 Flink SQL 共享许多概念以及部分集成的 API。通过查看[公共概念 & API]({{< ref "docs/dev/table/common" >}})来学习如何注册表或如何创建一个`表`对象。[流概念]({{< ref "docs/dev/table/concepts/overview" >}})页面讨论了诸如动态表和时间属性等流特有的概念。

下面的例子中假定有一张叫 `Orders` 的表，表中有属性 `(a, b, c, rowtime)` 。`rowtime` 字段是流任务中的逻辑[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})或是批任务中的普通时间戳字段。

概述 & 示例
-----------------------------

Table API 支持 Scala, Java 和 Python 语言。Scala 语言的 Table API 利用了 Scala 表达式，Java 语言的 Table API 支持 DSL 表达式和解析并转换为等价表达式的字符串，Python 语言的 Table API 仅支持解析并转换为等价表达式的字符串。

下面的例子展示了 Scala、Java 和 Python 语言的 Table API 的不同之处。表程序是在批环境下执行的。程序扫描了 `Orders` 表，通过字段 `a` 进行分组，并计算了每组结果的行数。

{{< tabs "8ffbc88b-54d7-4936-9a53-b63cb22c4a56" >}}
{{< tab "Java" >}}

 Java 的 Table API 通过引入 `org.apache.flink.table.api.java.*` 来使用。下面的例子展示了如何创建一个 Java 的 Table API 程序，以及表达式是如何指定为字符串的。
使用DSL表达式时也需要引入静态的 `org.apache.flink.table.api.Expressions.*`。

```java
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(settings);

// 在表环境中注册 Orders 表
// ...

// 指定表程序
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table counts = orders
        .groupBy($("a"))
        .select($("a"), $("b").count().as("cnt"));

// 打印
counts.execute().print();
```

{{< /tab >}}
{{< tab "Scala" >}}

Scala 的 Table API 通过引入 `org.apache.flink.table.api._`、`org.apache.flink.api.scala._` 和 `org.apache.flink.table.api.bridge.scala._`（开启数据流的桥接支持）来使用。

下面的例子展示了如何创建一个 Scala 的 Table API 程序。通过 Scala 的带美元符号（`$`）的字符串插值来实现表字段引用。

```scala
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

// 环境配置
val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

val tEnv = TableEnvironment.create(settings);

// 在表环境中注册 Orders 表
// ...

// 指定表程序
val orders = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result = orders
               .groupBy($"a")
               .select($"a", $"b".count as "cnt")
               .execute()
               .print()
```

{{< /tab >}}
{{< tab "Python" >}}

下面的例子展示了如何创建一个 Python 的 Table API 程序，以及表达式是如何指定为字符串的。

```python
from pyflink.table import *

# 环境配置
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())

# 在表环境中注册 Orders 表和结果 sink 表
source_data_path = "/path/to/source/directory/"
result_data_path = "/path/to/result/directory/"
source_ddl = f"""
        create table Orders(
            a VARCHAR,
            b BIGINT,
            c BIGINT,
            rowtime TIMESTAMP(3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path}'
        )
        """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    create table `Result`(
        a VARCHAR,
        cnt BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

# 指定表程序
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

orders.group_by("a").select(orders.a, orders.b.count.alias('cnt')).execute_insert("result").wait()

```

{{< /tab >}}
{{< /tabs >}}

下一个例子展示了一个更加复杂的 Table API 程序。这个程序也扫描 `Orders` 表。程序过滤了空值，使字符串类型的字段 `a` 标准化，并且每个小时进行一次计算并返回 `a` 的平均账单金额 `b`。

{{< tabs "6df651bc-ae06-44de-a36c-6a1e6d1b7355" >}}
{{< tab "Java" >}}

```java
// 环境配置
// ...

// 指定表程序
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table result = orders
        .filter(
            and(
                $("a").isNotNull(),
                $("b").isNotNull(),
                $("c").isNotNull()
            ))
        .select($("a").lowerCase().as("a"), $("b"), $("rowtime"))
        .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
        .groupBy($("hourlyWindow"), $("a"))
        .select($("a"), $("hourlyWindow").end().as("hour"), $("b").avg().as("avgBillingAmount"));
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// 环境配置
// ...

// 指定表程序
val orders: Table = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result: Table = orders
        .filter($"a".isNotNull && $"b".isNotNull && $"c".isNotNull)
        .select($"a".lowerCase() as "a", $"b", $"rowtime")
        .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
        .groupBy($"hourlyWindow", $"a")
        .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# 指定表程序
from pyflink.table.expressions import col, lit

orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

result = orders.filter(orders.a.is_not_null & orders.b.is_not_null & orders.c.is_not_null) \
               .select(orders.a.lower_case.alias('a'), orders.b, orders.rowtime) \
               .window(Tumble.over(lit(1).hour).on(orders.rowtime).alias("hourly_window")) \
               .group_by(col('hourly_window'), col('a')) \
               .select(col('a'), col('hourly_window').end.alias('hour'), b.avg.alias('avg_billing_amount'))
```

{{< /tab >}}
{{< /tabs >}}

因为 Table API 的批数据 API 和流数据 API 是统一的，所以这两个例子程序不需要修改代码就可以运行在流输入或批输入上。在这两种情况下，只要流任务没有数据延时，程序将会输出相同的结果（查看[流概念]({{< ref "docs/dev/table/concepts/overview" >}})获取详情)。

{{< top >}}

Operations
----------

Table API支持如下操作。请注意不是所有的操作都可以既支持流也支持批；这些操作都具有相应的标记。

### Scan, Projection, and Filter

#### From

{{< label "Batch" >}} {{< label "Streaming" >}}

和 SQL 查询的 `FROM` 子句类似。
执行一个注册过的表的扫描。

{{< tabs "from" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
```
{{< /tab >}}
{{< /tabs >}}

#### FromValues

{{< label "Batch" >}} {{< label "Streaming" >}}

和 SQL 查询中的 `VALUES` 子句类似。
基于提供的行生成一张内联表。

你可以使用 `row(...)` 表达式创建复合行：

{{< tabs "fromvalues" >}}
{{< tab "Java" >}}
```java
Table table = tEnv.fromValues(
   row(1, "ABC"),
   row(2L, "ABCDE")
);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table = tEnv.fromValues(
   row(1, "ABC"),
   row(2L, "ABCDE")
)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table = t_env.from_elements([(1, 'ABC'), (2, 'ABCDE')])
```
{{< /tab >}}
{{< /tabs >}}

这将生成一张结构如下的表：

```
root
|-- f0: BIGINT NOT NULL     // original types INT and BIGINT are generalized to BIGINT
|-- f1: VARCHAR(5) NOT NULL // original types CHAR(3) and CHAR(5) are generalized
                            // to VARCHAR(5). VARCHAR is used instead of CHAR so that
                            // no padding is applied
```

这个方法会根据输入的表达式自动获取类型。如果在某一个特定位置的类型不一致，该方法会尝试寻找一个所有类型的公共超类型。如果公共超类型不存在，则会抛出异常。

你也可以明确指定所需的类型。指定如 DECIMAL 这样的一般类型或者给列命名可能是有帮助的。

{{< tabs "fromvalueswithtype" >}}
{{< tab "Java" >}}
```java
Table table = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table = t_env.from_elements(
  [(1, 'ABC'), (2, 'ABCDE')],
  schema=DataTypes.Row([DataTypes.FIELD('id', DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD('name', DataTypes.STRING())]))
```
{{< /tab >}}
{{< /tabs >}}

这将生成一张结构如下的表：

```
root
|-- id: DECIMAL(10, 2)
|-- name: STRING
```

#### Select

{{< label "Batch" >}} {{< label "Streaming" >}}

和 SQL 的 `SELECT` 子句类似。
执行一个 select 操作。

{{< tabs "select" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.select($("a"), $("c").as("d"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
Table result = orders.select($"a", $"c" as "d");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.select(orders.a, orders.c.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

你可以选择星号`（*）`作为通配符，select 表中的所有列。

{{< tabs "selectstar" >}}
{{< tab "Java" >}}
```java
Table result = orders.select($("*"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
Table result = orders.select($"*")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

result = orders.select(col("*"))
```
{{< /tab >}}
{{< /tabs >}}

#### As

{{< label "Batch" >}} {{< label "Streaming" >}}

重命名字段。

{{< tabs "as" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.as("x, y, z, t");
```
{{< /tab >}}
{{< tab "scala" >}}
```java
val orders: Table = tableEnv.from("Orders").as("x", "y", "z", "t")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.alias("x, y, z, t")
```
{{< /tab >}}
{{< /tabs >}}

####  Where / Filter

{{< label "Batch" >}} {{< label "Streaming" >}}

和 SQL 的 `WHERE` 子句类似。
过滤掉未验证通过过滤谓词的行。

{{< tabs "where" >}}
{{< tab "Java" >}}

```java
Table orders = tableEnv.from("Orders");
Table result = orders.where($("b").isEqual("red"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.filter($"a" % 2 === 0)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.where(orders.a == 'red')
```
{{< /tab >}}
{{< /tabs >}}

或者

{{< tabs "filter" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.filter($("b").isEqual("red"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.filter($"a" % 2 === 0)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.filter(orders.a == 'red')
```
{{< /tab >}}
{{< /tabs >}}



<a name="columln-operations"></a>
### 列操作

#### AddColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

执行字段添加操作。
如果所添加的字段已经存在，将抛出异常。

{{< tabs "addcolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.addColumns(concat($("c"), "sunny"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.addColumns(concat($"c", "Sunny"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import concat

orders = t_env.from_path("Orders")
result = orders.add_columns(concat(orders.c, 'sunny'))
```
{{< /tab >}}
{{< /tabs >}}

#### AddOrReplaceColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

执行字段添加操作。
如果添加的列名称和已存在的列名称相同，则已存在的字段将被替换。
此外，如果添加的字段里面有重复的字段名，则会使用最后一个字段。

{{< tabs "addorreplacecolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.addOrReplaceColumns(concat($("c"), "sunny").as("desc"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.addOrReplaceColumns(concat($"c", "Sunny") as "desc")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import concat

orders = t_env.from_path("Orders")
result = orders.add_or_replace_columns(concat(orders.c, 'sunny').alias('desc'))
```
{{< /tab >}}
{{< /tabs >}}

#### DropColumns 

{{< label "Batch" >}} {{< label "Streaming" >}}

{{< tabs "dropcolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.dropColumns($("b"), $("c"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.dropColumns($"b", $"c")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.drop_columns(orders.b, orders.c)
```
{{< /tab >}}
{{< /tabs >}}

#### RenameColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

执行字段重命名操作。
字段表达式应该是别名表达式，并且仅当字段已存在时才能被重命名。

{{< tabs "renamecolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.renameColumns($("b").as("b2"), $("c").as("c2"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.renameColumns($"b" as "b2", $"c" as "c2")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.rename_columns(orders.b.alias('b2'), orders.c.alias('c2'))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Aggregations

#### GroupBy Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

和 SQL 的 `GROUP BY` 子句类似。
使用分组键对行进行分组，使用伴随的聚合算子来按照组进行聚合行。

{{< tabs "groupby" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.groupBy($("a")).select($("a"), $("b").sum().as("d"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.groupBy($"a").select($"a", $"b".sum().as("d"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.group_by(orders.a).select(orders.a, orders.b.sum.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### GroupBy Window Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}

使用[分组窗口](#group-windows)结合单个或者多个分组键对表进行分组和聚合。 

{{< tabs "groupbywindow" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders
    .window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w")) // 定义窗口
    .groupBy($("a"), $("w")) // 按窗口和键分组
    // 访问窗口属性并聚合
    .select(
        $("a"),
        $("w").start(),
        $("w").end(),
        $("w").rowtime(),
        $("b").sum().as("d")
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w") // 定义窗口
    .groupBy($"a", $"w") // 按窗口和键分组
    .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".sum as "d") // 访问窗口属性并聚合
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col

orders = t_env.from_path("Orders")
result = orders.window(Tumble.over(lit(5).minutes).on(orders.rowtime).alias("w")) \ 
               .group_by(orders.a, col('w')) \
               .select(orders.a, col('w').start, col('w').end, orders.b.sum.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

#### Over Window Aggregation

和 SQL 的 `OVER` 子句类似。
更多细节详见 [over windows section](#over-windows)

{{< tabs "overwindowagg" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders
    // 定义窗口
    .window(
        Over
          .partitionBy($("a"))
          .orderBy($("rowtime"))
          .preceding(UNBOUNDED_RANGE)
          .following(CURRENT_RANGE)
          .as("w"))
    // 滑动聚合
    .select(
        $("a"),
        $("b").avg().over($("w")),
        $("b").max().over($("w")),
        $("b").min().over($("w"))
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    // 定义窗口
    .window(
        Over
          partitionBy $"a"
          orderBy $"rowtime"
          preceding UNBOUNDED_RANGE
          following CURRENT_RANGE
          as "w")
    .select($"a", $"b".avg over $"w", $"b".max().over($"w"), $"b".min().over($"w")) // 滑动聚合
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.window import Over
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE

orders = t_env.from_path("Orders")
result = orders.over_window(Over.partition_by(orders.a).order_by(orders.rowtime)
                            .preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE)
                            .alias("w")) \
               .select(orders.a, orders.b.avg.over(col('w')), orders.b.max.over(col('w')), orders.b.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

所有的聚合必须定义在同一个窗口上，比如同一个分区、排序和范围内。目前只支持 PRECEDING 到当前行范围（无界或有界）的窗口。尚不支持 FOLLOWING 范围的窗口。ORDER BY 操作必须指定一个单一的[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})。

#### Distinct Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

和 SQL DISTINCT 聚合子句类似，例如 `COUNT(DISTINCT a)`。
Distinct 聚合声明的聚合函数（内置或用户定义的）仅应用于互不相同的输入值。
Distinct 可以应用于 **GroupBy Aggregation**、**GroupBy Window Aggregation** 和 **Over Window Aggregation**。

{{< tabs "distinctagg" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
// 按属性分组后的的互异（互不相同、去重）聚合
Table groupByDistinctResult = orders
    .groupBy($("a"))
    .select($("a"), $("b").sum().distinct().as("d"));
// 按属性、时间窗口分组后的互异（互不相同、去重）聚合
Table groupByWindowDistinctResult = orders
    .window(Tumble
            .over(lit(5).minutes())
            .on($("rowtime"))
            .as("w")
    )
    .groupBy($("a"), $("w"))
    .select($("a"), $("b").sum().distinct().as("d"));
// over window 上的互异（互不相同、去重）聚合
Table result = orders
    .window(Over
        .partitionBy($("a"))
        .orderBy($("rowtime"))
        .preceding(UNBOUNDED_RANGE)
        .as("w"))
    .select(
        $("a"), $("b").avg().distinct().over($("w")),
        $("b").max().over($("w")),
        $("b").min().over($("w"))
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders");
// 按属性分组后的的互异（互不相同、去重）聚合
val groupByDistinctResult = orders
    .groupBy($"a")
    .select($"a", $"b".sum.distinct as "d")
// 按属性、时间窗口分组后的互异（互不相同、去重）聚合
val groupByWindowDistinctResult = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w").groupBy($"a", $"w")
    .select($"a", $"b".sum.distinct as "d")
// over window 上的互异（互不相同、去重）聚合
val result = orders
    .window(Over
        partitionBy $"a"
        orderBy $"rowtime"
        preceding UNBOUNDED_RANGE
        as $"w")
    .select($"a", $"b".avg.distinct over $"w", $"b".max over $"w", $"b".min over $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col, lit, UNBOUNDED_RANGE

orders = t_env.from_path("Orders")
# 按属性分组后的的互异（互不相同、去重）聚合
group_by_distinct_result = orders.group_by(orders.a) \
                                 .select(orders.a, orders.b.sum.distinct.alias('d'))
# 按属性、时间窗口分组后的互异（互不相同、去重）聚合
group_by_window_distinct_result = orders.window(
    Tumble.over(lit(5).minutes).on(orders.rowtime).alias("w")).group_by(orders.a, col('w')) \
    .select(orders.a, orders.b.sum.distinct.alias('d'))
# over window 上的互异（互不相同、去重）聚合
result = orders.over_window(Over
                       .partition_by(orders.a)
                       .order_by(orders.rowtime)
                       .preceding(UNBOUNDED_RANGE)
                       .alias("w")) \
                       .select(orders.a, orders.b.avg.distinct.over(col('w')), orders.b.max.over(col('w')), orders.b.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

用户定义的聚合函数也可以与 `DISTINCT` 修饰符一起使用。如果计算不同（互异、去重的）值的聚合结果，则只需向聚合函数添加 distinct 修饰符即可。

{{< tabs "distinctudf" >}}
{{< tab "Java" >}}
```java
Table orders = tEnv.from("Orders");

// 对 user-defined aggregate functions 使用互异（互不相同、去重）聚合
tEnv.registerFunction("myUdagg", new MyUdagg());
orders.groupBy("users")
    .select(
        $("users"),
        call("myUdagg", $("points")).distinct().as("myDistinctResult")
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tEnv.from("Orders");

// 对 user-defined aggregate functions 使用互异（互不相同、去重）聚合
val myUdagg = new MyUdagg();
orders.groupBy($"users").select($"users", myUdagg.distinct($"points") as "myDistinctResult");
```
{{< /tab >}}
{{< tab "Python" >}}
Unsupported
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Distinct

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

和 SQL 的 `DISTINCT` 子句类似。
返回具有不同组合值的记录。

{{< tabs "distinct" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.distinct();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.distinct()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.distinct()
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

{{< top >}}

### Joins

#### Inner Join

{{< label "Batch" >}} {{< label "Streaming" >}}

和 SQL 的 JOIN 子句类似。关联两张表。两张表必须有不同的字段名，并且必须通过 join 算子或者使用 where 或 filter 算子定义至少一个 join 等式连接谓词。

{{< tabs "innerjoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("MyTable).select($("a"), $("b"), $("c"));
Table right = tableEnv.from("MyTable).select($("d"), $("e"), $("f"));
Table result = left.join(right)
    .where($("a").isEqual($("d")))
    .select($("a"), $("b"), $("e"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f")
val result = left.join(right).where($"a" === $"d").select($"a", $"b", $"e")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'))
result = left.join(right).where(left.a == right.d).select(left.a, left.b, right.e)
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Outer Join
{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

和 SQL `LEFT`/`RIGHT`/`FULL OUTER JOIN` 子句类似。
关联两张表。
两张表必须有不同的字段名，并且必须定义至少一个等式连接谓词。

{{< tabs "outerjoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("MyTable).select($("a"), $("b"), $("c"));
Table right = tableEnv.from("MyTable).select($("d"), $("e"), $("f"));

Table leftOuterResult = left.leftOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
Table rightOuterResult = left.rightOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
Table fullOuterResult = left.fullOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f")

val leftOuterResult = left.leftOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val rightOuterResult = left.rightOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val fullOuterResult = left.fullOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'))

left_outer_result = left.left_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
right_outer_result = left.right_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
full_outer_result = left.full_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Interval Join

{{< label "Batch" >}} {{< label "Streaming" >}}

Interval join 是可以通过流模式处理的常规 join 的子集。

Interval join 至少需要一个 equi-join 谓词和一个限制双方时间界限的 join 条件。这种条件可以由两个合适的范围谓词（`<、<=、>=、>`）或一个比较两个输入表相同时间属性（即处理时间或事件时间）的等值谓词来定义。

{{< tabs "intervaljoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("MyTable).select($("a"), $("b"), $("c"), $("ltime"));
Table right = tableEnv.from("MyTable).select($("d"), $("e"), $("f"), $("rtime"));

Table result = left.join(right)
  .where(
    and(
        $("a").isEqual($("d")),
        $("ltime").isGreaterOrEqual($("rtime").minus(lit(5).minutes())),
        $("ltime").isLess($("rtime").plus(lit(10).minutes()))
    ))
  .select($("a"), $("b"), $("e"), $("ltime"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c", $"ltime")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f", $"rtime")

val result = left.join(right)
  .where($"a" === $"d" && $"ltime" >= $"rtime" - 5.minutes && $"ltime" < $"rtime" + 10.minutes)
  .select($"a", $"b", $"e", $"ltime")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'), col('rowtime1'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'), col('rowtime2'))
  
joined_table = left.join(right).where((left.a == right.d) & (left.rowtime1 >= right.rowtime2 - lit(1).second) & (left.rowtime1 <= right.rowtime2 + lit(2).seconds))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.e, joined_table.rowtime1)
```
{{< /tab >}}
{{< /tabs >}}

#### Inner Join with Table Function (UDTF)

{{< label "Batch" >}} {{< label "Streaming" >}}

join 表和表函数的结果。左（外部）表的每一行都会 join 表函数相应调用产生的所有行。
如果表函数调用返回空结果，则删除左侧（外部）表的一行。

{{< tabs "udtf" >}}
{{< tab "Java" >}}
```java
// 注册 User-Defined Table Function
TableFunction<Tuple3<String,String,String>> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.from("Orders");
Table result = orders
    .joinLateral(call("split", $("c")).as("s", "t", "v"))
    .select($("a"), $("b"), $("s"), $("t"), $("v"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 实例化 User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .joinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 注册 User-Defined Table Function
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def split(x):
    return [Row(1, 2, 3)]

# join
orders = t_env.from_path("Orders")
joined_table = orders.join_lateral(split(orders.c).alias("s, t, v"))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.s, joined_table.t, joined_table.v)
```
{{< /tab >}}
{{< /tabs >}}


####  Left Outer Join with Table Function (UDTF)

{{< label "Batch" >}} {{< label "Streaming" >}}

join 表和表函数的结果。左（外部）表的每一行都会 join 表函数相应调用产生的所有行。如果表函数调用返回空结果，则保留相应的 outer（外部连接）行并用空值填充右侧结果。

目前，表函数左外连接的谓词只能为空或字面（常量）真。

{{< tabs "outerudtf" >}}
{{< tab "Java" >}}
```java
// 注册 User-Defined Table Function
TableFunction<Tuple3<String,String,String>> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.from("Orders");
Table result = orders
    .leftOuterJoinLateral(call("split", $("c")).as("s", "t", "v"))
    .select($("a"), $("b"), $("s"), $("t"), $("v"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 实例化 User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .leftOuterJoinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 注册 User-Defined Table Function
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def split(x):
    return [Row(1, 2, 3)]

# join
orders = t_env.from_path("Orders")
joined_table = orders.left_outer_join_lateral(split(orders.c).alias("s, t, v"))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.s, joined_table.t, joined_table.v)
```
{{< /tab >}}
{{< /tabs >}}

#### Join with Temporal Table

Temporal table 是跟踪随时间变化的表。

Temporal table 函数提供对特定时间点 temporal table 状态的访问。表与 temporal table 函数进行 join 的语法和使用表函数进行 inner join 的语法相同。

目前仅支持与 temporal table 的 inner join。

{{< tabs "temporaltablefunc" >}}
{{< tab "Java" >}}
```java
Table ratesHistory = tableEnv.from("RatesHistory");

// 注册带有时间属性和主键的 temporal table function
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
    "r_proctime",
    "r_currency");
tableEnv.registerFunction("rates", rates);

// 基于时间属性和键与“Orders”表关联
Table orders = tableEnv.from("Orders");
Table result = orders
    .joinLateral(call("rates", $("o_proctime")), $("o_currency").isEqual($("r_currency")));
{{< /tab >}}
{{< tabs "Scala" >}}
​```scala
val ratesHistory = tableEnv.from("RatesHistory")

// 注册带有时间属性和主键的 temporal table function
val rates = ratesHistory.createTemporalTableFunction($"r_proctime", $"r_currency")

// 基于时间属性和键与“Orders”表关联
val orders = tableEnv.from("Orders")
val result = orders
    .joinLateral(rates($"o_rowtime"), $"r_currency" === $"o_currency")
```
{{< /tabs >}}
{{< tab "Python" >}}
目前不支持 Python 的 Table API。
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Set Operations

#### Union

{{< label Batch >}}

和 SQL `UNION` 子句类似。Union 两张表会删除重复记录。两张表必须具有相同的字段类型。

{{< tabs "union" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.union(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.union(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.union(right)
{{< /tab >}}
{{< /tabs >}}

#### UnionAll

{{< label Batch >}} {{< label Streaming >}}


和 SQL `UNION ALL` 子句类似。Union 两张表。
两张表必须具有相同的字段类型。

{{< tabs "unionall" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.unionAll(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.unionAll(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.unionAll(right)
{{< /tab >}}
{{< /tabs >}}

#### Intersect 

{{< label Batch >}}

和 SQL `INTERSECT` 子句类似。Intersect 返回两个表中都存在的记录。如果一条记录在一张或两张表中存在多次，则只返回一条记录，也就是说，结果表中不存在重复的记录。两张表必须具有相同的字段类型。

{{< tabs "intersect" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.intersect(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersect(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.intersect(right)
{{< /tab >}}
{{< /tabs >}}

#### IntersectAll

{{< label Batch >}}

和 SQL `INTERSECT ALL` 子句类似。IntersectAll 返回两个表中都存在的记录。如果一条记录在两张表中出现多次，那么该记录返回的次数同该记录在两个表中都出现的次数一致，也就是说，结果表可能存在重复记录。两张表必须具有相同的字段类型。

{{< tabs "intersectAll" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.intersectAll(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersectAll(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.intersectAll(right)
{{< /tab >}}
{{< /tabs >}}

#### Minus 

{{< label Batch  >}}

和 SQL `EXCEPT` 子句类似。Minus 返回左表中存在且右表中不存在的记录。左表中的重复记录只返回一次，换句话说，结果表中没有重复记录。两张表必须具有相同的字段类型。

{{< tabs "minus" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.minus(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minus(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.minus(right)
{{< /tab >}}
{{< /tabs >}}

#### MinusAll
{{< label Batch >}}

和 SQL `EXCEPT ALL` 子句类似。MinusAll 返回右表中不存在的记录。在左表中出现 n 次且在右表中出现 m 次的记录，在结果表中出现 (n - m) 次，例如，也就是说结果中删掉了在右表中存在重复记录的条数的记录。两张表必须具有相同的字段类型。

{{< tabs "minusall" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.minusAll(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minusAll(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.minusAll(right)
{{< /tab >}}
{{< /tabs >}}

#### In 

{{< label Batch >}} {{< label Streaming >}}

和 SQL `IN` 子句类似。如果表达式的值存在于给定表的子查询中，那么 In 子句返回 true。子查询表必须由一列组成。这个列必须与表达式具有相同的数据类型。

{{< tabs "in" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("Orders1")
Table right = tableEnv.from("Orders2");

Table result = left.select($("a"), $("b"), $("c")).where($("a").in(right));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("Orders1")
val right = tableEnv.from("Orders2");

val result = left.select($"a", $"b", $"c").where($"a".in(right))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('a'))

result = left.select(left.a, left.b, left.c).where(left.a.in_(right))
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

{{< top >}}

### OrderBy, Offset & Fetch

#### Order By 

{{< label Batch >}} {{< label Streaming >}}

和 SQL `ORDER BY` 子句类似。返回跨所有并行分区的全局有序记录。对于无界表，该操作需要对时间属性进行排序或进行后续的 fetch 操作。

{{< tabs "orderby" >}}
{{< tab "Java" >}}
```java
Table result = in.orderBy($("a").asc());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val result = in.orderBy($"a".asc)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
result = in.order_by(in.a.asc)
```
{{< /tab >}}
{{< /tabs >}}

#### Offset & Fetch 

{{< label Batch >}} {{< label Streaming >}}

和 SQL 的 `OFFSET`  和 `FETCH` 子句类似。Offset 操作根据偏移位置来限定（可能是已排序的）结果集。Fetch 操作将（可能已排序的）结果集限制为前 n 行。通常，这两个操作前面都有一个排序操作。对于无界表，offset 操作需要 fetch 操作。

{{< tabs "offsetfetch" >}}
{{< tab "Java" >}}
```java
// 从已排序的结果集中返回前5条记录
Table result1 = in.orderBy($("a").asc()).fetch(5);

// 从已排序的结果集中返回跳过3条记录之后的所有记录
Table result2 = in.orderBy($("a").asc()).offset(3);

// 从已排序的结果集中返回跳过10条记录之后的前5条记录
Table result3 = in.orderBy($("a").asc()).offset(10).fetch(5);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

// 从已排序的结果集中返回前5条记录
val result1: Table = in.orderBy($"a".asc).fetch(5)

// 从已排序的结果集中返回跳过3条记录之后的所有记录
val result2: Table = in.orderBy($"a".asc).offset(3)

// 从已排序的结果集中返回跳过10条记录之后的前5条记录
val result3: Table = in.orderBy($"a".asc).offset(10).fetch(5)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 从已排序的结果集中返回前5条记录
result1 = table.order_by(table.a.asc).fetch(5)

# 从已排序的结果集中返回跳过3条记录之后的所有记录
result2 = table.order_by(table.a.asc).offset(3)

# 从已排序的结果集中返回跳过10条记录之后的前5条记录
result3 = table.order_by(table.a.asc).offset(10).fetch(5)
```
{{< /tab >}}
{{< /tabs >}}

### Insert

{{< label Batch >}} {{< label Streaming >}}

和 SQL 查询中的 `INSERT INTO` 子句类似，该方法执行对已注册的输出表的插入操作。`executeInsert()` 方法将立即提交执行插入操作的 Flink job。

输出表必须已注册在 TableEnvironment（详见表连接器）中。此外，已注册表的 schema 必须与查询中的 schema 相匹配。

{{< tabs "insertinto" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
orders.executeInsert("OutOrders");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
orders.executeInsert("OutOrders")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
orders.execute_insert("OutOrders")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Group Windows

Group window 聚合根据时间或行计数间隔将行分为有限组，并为每个分组进行一次聚合函数计算。对于批处理表，窗口是按时间间隔对记录进行分组的便捷方式。

{{< tabs "248a1eb3-c75a-404e-957e-08a012cbed51" >}}
{{< tab "Java" >}}
窗口是使用 `window(GroupWindow w)` 子句定义的，并且需要使用 `as` 子句来指定别名。为了按窗口对表进行分组，窗口别名的引用必须像常规分组属性一样在 `groupBy(...)` 子句中。
以下示例展示了如何在表上定义窗口聚合。

```java
Table table = input
  .window([GroupWindow w].as("w"))  // 定义窗口并指定别名为 w
  .groupBy($("w"))  // 以窗口 w 对表进行分组
  .select($("b").sum());  // 聚合
```
{{< /tab >}}
{{< tab "Scala" >}}
窗口是使用 `window(GroupWindow w)` 子句定义的，并且需要使用 `as` 子句来指定别名。为了按窗口对表进行分组，窗口别名必须像常规分组属性一样在 `groupBy(...)` 子句中引用。
以下示例展示了如何在表上定义窗口聚合。

```scala
val table = input
  .window([w: GroupWindow] as $"w")  // 定义窗口并指定别名为 w
  .groupBy($"w")   // 以窗口 w 对表进行分组
  .select($"b".sum)  // 聚合
```
{{< /tab >}}
{{< tab "Python" >}}
窗口是使用 `window(GroupWindow w)` 子句定义的，并且需要使用 `alias` 子句来指定别名。为了按窗口对表进行分组，窗口别名必须像常规分组属性一样在 `group_by(...)` 子句中引用。
以下示例展示了如何在表上定义窗口聚合。

```python
# 定义窗口并指定别名为 w，以窗口 w 对表进行分组，然后再聚合
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w')).select(input.b.sum)
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "3a855a09-96d3-4dd5-9cbe-07b7f3dc4af9" >}}
{{< tab "Java" >}}
在流环境中，如果窗口聚合除了窗口之外还根据一个或多个属性进行分组，则它们只能并行计算，例如，`groupBy(...)` 子句引用了一个窗口别名和至少一个附加属性。仅引用窗口别名（例如在上面的示例中）的 `groupBy(...)` 子句只能由单个非并行任务进行计算。
以下示例展示了如何定义有附加分组属性的窗口聚合。

```java
Table table = input
  .window([GroupWindow w].as("w"))  // 定义窗口并指定别名为 w
  .groupBy($("w"), $("a"))  // 以属性 a 和窗口 w 对表进行分组
  .select($("a"), $("b").sum());  // 聚合
```
{{< /tab >}}
{{< tab "Scala" >}}
在流环境中，如果窗口聚合除了窗口之外还根据一个或多个属性进行分组，则它们只能并行计算，例如，`groupBy(...)` 子句引用了一个窗口别名和至少一个附加属性。仅引用窗口别名（例如在上面的示例中）的 `groupBy(...)` 子句只能由单个非并行任务进行计算。
以下示例展示了如何定义有附加分组属性的窗口聚合。

```scala
val table = input
  .window([w: GroupWindow] as $"w") // 定义窗口并指定别名为 w
  .groupBy($"w", $"a")  // 以属性 a 和窗口 w 对表进行分组
  .select($"a", $"b".sum)  // 聚合
```
{{< /tab >}}
{{< tab "Python" >}}
在流环境中，如果窗口聚合除了窗口之外还根据一个或多个属性进行分组，则它们只能并行计算，例如，`group_by(...)` 子句引用了一个窗口别名和至少一个附加属性。仅引用窗口别名（例如在上面的示例中）的 `group_by(...)` 子句只能由单个非并行任务进行计算。
以下示例展示了如何定义有附加分组属性的窗口聚合。

```python
# 定义窗口并指定别名为 w，以属性 a 和窗口 w 对表进行分组，
# 然后再聚合
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w'), input.a).select(input.b.sum)
```
{{< /tab >}}
{{< /tabs >}}

时间窗口的开始、结束或行时间戳等窗口属性可以作为窗口别名的属性添加到 select 子句中，如 `w.start`、`w.end` 和 `w.rowtime`。窗口开始和行时间戳是包含的上下窗口边界。相反，窗口结束时间戳是唯一的上窗口边界。例如，从下午 2 点开始的 30 分钟滚动窗口将 “14:00:00.000” 作为开始时间戳，“14:29:59.999” 作为行时间时间戳，“14:30:00.000” 作为结束时间戳。

{{< tabs "1397cfe2-8ed8-4a39-938c-f2c066c2bdcf" >}}
{{< tab "Java" >}}
```java
Table table = input
  .window([GroupWindow w].as("w"))  // 定义窗口并指定别名为 w
  .groupBy($("w"), $("a"))  // 以属性 a 和窗口 w 对表进行分组
  .select($("a"), $("w").start(), $("w").end(), $("w").rowtime(), $("b").count()); // 聚合并添加窗口开始、结束和 rowtime 时间戳
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = input
  .window([w: GroupWindow] as $"w")  // 定义窗口并指定别名为 w
  .groupBy($"w", $"a")  // 以属性 a 和窗口 w 对表进行分组
  .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".count) // 聚合并添加窗口开始、结束和 rowtime 时间戳
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 定义窗口并指定别名为 w，以属性 a 和窗口 w 对表进行分组，
# 然后再聚合并添加窗口开始、结束和 rowtime 时间戳
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w'), input.a) \
             .select(input.a, col('w').start, col('w').end, col('w').rowtime, input.b.count)
```
{{< /tab >}}
{{< /tabs >}}

`Window` 参数定义了如何将行映射到窗口。 `Window` 不是用户可以实现的接口。相反，Table API 提供了一组具有特定语义的预定义 `Window` 类。下面列出了支持的窗口定义。

#### Tumble (Tumbling Windows)

滚动窗口将行分配给固定长度的非重叠连续窗口。例如，一个 5 分钟的滚动窗口以 5 分钟的间隔对行进行分组。滚动窗口可以定义在事件时间、处理时间或行数上。

{{< tabs "96f964cc-d78c-493b-b190-19cab37a8031" >}}
{{< tab "Java" >}}
滚动窗口是通过 `Tumble` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```java
// Tumbling Event-time Window
.window(Tumble.over(lit(10).minutes()).on($("rowtime")).as("w"));

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(lit(10).minutes()).on($("proctime")).as("w"));

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(rowInterval(10)).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
滚动窗口是通过 `Tumble` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```scala
// Tumbling Event-time Window
.window(Tumble over 10.minutes on $"rowtime" as $"w")

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.minutes on $"proctime" as $"w")

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.rows on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
滚动窗口是通过 `Tumble` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>指定窗口的别名。别名用于在 <code>group_by()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```python
# Tumbling Event-time Window
.window(Tumble.over(lit(10).minutes).on(col('rowtime')).alias("w"))

# Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(lit(10).minutes).on(col('proctime')).alias("w"))

# Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(row_interval(10)).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Slide (Sliding Windows)

滑动窗口具有固定大小并按指定的滑动间隔滑动。如果滑动间隔小于窗口大小，则滑动窗口重叠。因此，行可能分配给多个窗口。例如，15 分钟大小和 5 分钟滑动间隔的滑动窗口将每一行分配给 3 个不同的 15 分钟大小的窗口，以 5 分钟的间隔进行一次计算。滑动窗口可以定义在事件时间、处理时间或行数上。

{{< tabs "8a408c78-2a76-4193-a457-e95af384edb5" >}}
{{< tab "Java" >}}
滑动窗口是通过 `Slide` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。滑动间隔的类型必须与窗口长度的类型相同。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```java
// Sliding Event-time Window
.window(Slide.over(lit(10).minutes())
            .every(lit(5).minutes())
            .on($("rowtime"))
            .as("w"));

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over(lit(10).minutes())
            .every(lit(5).minutes())
            .on($("proctime"))
            .as("w"));

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
滑动窗口是通过 `Slide` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。滑动间隔的类型必须与窗口长度的类型相同。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```scala
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on $"rowtime" as $"w")

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide over 10.minutes every 5.minutes on $"proctime" as $"w")

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide over 10.rows every 5.rows on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
滑动窗口是通过 `Slide` 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>将窗口的长度定义为时间或行计数间隔。滑动间隔的类型必须与窗口长度的类型相同。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>指定窗口的别名。别名用于在 <code>group_by()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```python
# Sliding Event-time Window
.window(Slide.over(lit(10).minutes).every(lit(5).minutes).on(col('rowtime')).alias("w"))

# Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over(lit(10).minutes).every(lit(5).minutes).on(col('proctime')).alias("w"))

# Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over(row_interval(10)).every(row_interval(5)).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Session (Session Windows)

会话窗口没有固定的大小，其边界是由不活动的间隔定义的，例如，如果在定义的间隔期内没有事件出现，则会话窗口将关闭。例如，定义30 分钟间隔的会话窗口，当观察到一行在 30 分钟内不活动（否则该行将被添加到现有窗口中）且30 分钟内没有添加新行，窗口会关闭。会话窗口支持事件时间和处理时间。

{{< tabs "58943253-807b-4e4c-b068-0dc1b783b7b5" >}}
{{< tab "Java" >}}
会话窗口是通过 Session 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>将两个窗口之间的间隙定义为时间间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```java
// Session Event-time Window
.window(Session.withGap(lit(10).minutes()).on($("rowtime")).as("w"));

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.withGap(lit(10).minutes()).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
会话窗口是通过 Session 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>将两个窗口之间的间隙定义为时间间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>指定窗口的别名。别名用于在 <code>groupBy()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```scala
// Session Event-time Window
.window(Session withGap 10.minutes on $"rowtime" as $"w")

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session withGap 10.minutes on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
会话窗口是通过 Session 类定义的，具体如下：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>with_gap</code></td>
      <td>将两个窗口之间的间隙定义为时间间隔。</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>要对数据进行分组（时间间隔）或排序（行计数）的时间属性。批处理查询支持任意 Long 或 Timestamp 类型的属性。流处理查询仅支持<a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">声明的事件时间或处理时间属性</a>。</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>指定窗口的别名。别名用于在 <code>group_by()</code> 子句中引用窗口，并可以在 <code>select()</code> 子句中选择如窗口开始、结束或行时间戳的窗口属性。</td>
    </tr>
  </tbody>
</table>

```python
# Session Event-time Window
.window(Session.with_gap(lit(10).minutes).on(col('rowtime')).alias("w"))

# Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.with_gap(lit(10).minutes).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Over Windows

Over window 聚合聚合来自在标准的 SQL（`OVER` 子句），可以在 `SELECT` 查询子句中定义。与在“GROUP BY”子句中指定的 group window 不同， over window 不会折叠行。相反，over window 聚合为每个输入行在其相邻行的范围内计算聚合。

Over windows 使用 `window(w: OverWindow*)` 子句（在 Python API 中使用 `over_window(*OverWindow)`）定义，并通过 `select()` 方法中的别名引用。以下示例显示如何在表上定义 over window 聚合。

{{< tabs "92e08076-6823-451b-b54f-8e58c1b54dc3" >}}
{{< tab "Java" >}}
```java
Table table = input
  .window([OverWindow w].as("w"))           // define over window with alias w
  .select($("a"), $("b").sum().over($("w")), $("c").min().over($("w"))); // aggregate over the over window w
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = input
  .window([w: OverWindow] as $"w")              // define over window with alias w
  .select($"a", $"b".sum over $"w", $"c".min over $"w") // aggregate over the over window w
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# define over window with alias w and aggregate over the over window w
table = input.over_window([w: OverWindow].alias("w")) \
    .select(input.a, input.b.sum.over(col('w')), input.c.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

`OverWindow` 定义了计算聚合的行范围。`OverWindow` 不是用户可以实现的接口。相反，Table API 提供了`Over` 类来配置 over window 的属性。可以在事件时间或处理时间以及指定为时间间隔或行计数的范围内定义 over window 。可以通过 `Over` 类（和其他类）上的方法来定义 over window，具体如下：

#### Partition By 

**可选的**

在一个或多个属性上定义输入的分区。每个分区单独排序，聚合函数分别应用于每个分区。

注意：在流环境中，如果窗口包含 partition by 子句，则只能并行计算 over window 聚合。如果没有 partitionBy(...)，数据流将由单个非并行任务处理。

#### Order By 

**必须的**

定义每个分区内行的顺序，从而定义聚合函数应用于行的顺序。

注意：对于流处理查询，必须声明事件时间或处理时间属性。目前，仅支持单个排序属性。

#### Preceding

**可选的**

定义了包含在窗口中并位于当前行之前的行的间隔。间隔可以是时间或行计数间隔。

有界 over window 用间隔的大小指定，例如，时间间隔为10分钟或行计数间隔为10行。

无界 over window 通过常量来指定，例如，用UNBOUNDED_RANGE指定时间间隔或用 UNBOUNDED_ROW 指定行计数间隔。无界 over windows 从分区的第一行开始。

如果省略前面的子句，则使用 UNBOUNDED_RANGE 和 CURRENT_RANGE 作为窗口前后的默认值。

#### Following

**可选的**

定义包含在窗口中并在当前行之后的行的窗口间隔。间隔必须以与前一个间隔（时间或行计数）相同的单位指定。

目前，不支持在当前行之后有行的 over window。相反，你可以指定两个常量之一：

* `CURRENT_ROW` 将窗口的上限设置为当前行。
* `CURRENT_RANGE` 将窗口的上限设置为当前行的排序键，例如，与当前行具有相同排序键的所有行都包含在窗口中。

如果省略后面的子句，则时间间隔窗口的上限定义为 `CURRENT_RANGE`，行计数间隔窗口的上限定义为CURRENT_ROW。

#### As

**必须的**

为 over window 指定别名。别名用于在之后的 `select()` 子句中引用该 over window。

注意：目前，同一个 select() 调用中的所有聚合函数必须在同一个 over window 上计算。

#### Unbounded Over Windows

{{< tabs "ff010e04-fbb2-461b-823f-13c186c0df4a" >}}
{{< tab "Java" >}}
```java
// 无界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w"));

// 无界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over.partitionBy($("a")).orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"));

// 无界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_ROW).as("w"));
 
// 无界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(UNBOUNDED_ROW).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 无界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_RANGE as "w")

// 无界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_RANGE as "w")

// 无界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_ROW as "w")
 
// 无界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_ROW as "w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 无界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(UNBOUNDED_RANGE).alias("w"))

# 无界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(UNBOUNDED_RANGE).alias("w"))

# 无界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(UNBOUNDED_ROW).alias("w"))
 
# 无界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(UNBOUNDED_ROW).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Bounded Over Windows
{{< tabs "221fc3e4-a1b0-4d0a-a745-bd1e89e926f7" >}}
{{< tab "Java" >}}
```java
// 有界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"));

// 有界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(lit(1).minutes()).as("w"));

// 有界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(rowInterval(10)).as("w"));
 
// 有界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(rowInterval(10)).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 有界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 1.minutes as "w")

// 有界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over partitionBy $"a" orderBy $"proctime" preceding 1.minutes as "w")

// 有界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 10.rows as "w")
  
// 有界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.window(Over partitionBy $"a" orderBy $"proctime" preceding 10.rows as "w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# 有界的事件时间 over window（假定有一个叫“rowtime”的事件时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(lit(1).minutes).alias("w"))

# 有界的处理时间 over window（假定有一个叫“proctime”的处理时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(lit(1).minutes).alias("w"))

# 有界的事件时间行数 over window（假定有一个叫“rowtime”的事件时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(row_interval(10)).alias("w"))
 
# 有界的处理时间行数 over window（假定有一个叫“proctime”的处理时间属性）
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(row_interval(10)).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Row-based Operations

基于行生成多列输出的操作。

#### Map

{{< label Batch >}} {{< label Streaming >}}

{{< tabs "map" >}}
{{< tab "Java" >}}

使用用户定义的标量函数或内置标量函数执行 map 操作。如果输出类型是复合类型，则输出将被展平。

```java
public class MyMapFunction extends ScalarFunction {
    public Row eval(String a) {
        return Row.of(a, "pre-" + a);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.ROW(Types.STRING(), Types.STRING());
    }
}

ScalarFunction func = new MyMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .map(call("func", $("c")).as("a", "b"));
```
{{< /tab >}}
{{< tab "Scala" >}}

使用用户定义的标量函数或内置标量函数执行 map 操作。如果输出类型是复合类型，则输出将被展平。

```scala
class MyMapFunction extends ScalarFunction {
  def eval(a: String): Row = {
    Row.of(a, "pre-" + a)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.STRING)
}

val func = new MyMapFunction()
val table = input
  .map(func($"c")).as("a", "b")
```
{{< /tab >}}
{{< tab "Python" >}}

使用 python 的[通用标量函数]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#scalar-functions)或[向量化标量函数]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-scalar-functions)执行 map 操作。如果输出类型是复合类型，则输出将被展平。

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import udf

def map_function(a: Row) -> Row:
    return Row(a.a + 1, a.b * a.b)

# 使用 python 通用标量函数进行 map 操作
func = udf(map_function, result_type=DataTypes.ROW(
                                     [DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.BIGINT())]))
table = input.map(func).alias('a', 'b')

# 使用 python 向量化标量函数进行 map 操作
pandas_func = udf(lambda x: x * 2, result_type=DataTypes.ROW(
                                                    [DataTypes.FIELD("a", DataTypes.BIGINT()),
                                                    DataTypes.FIELD("b", DataTypes.BIGINT()))]),
                  func_type='pandas')

table = input.map(pandas_func).alias('a', 'b')
```

{{< /tab >}}
{{< /tabs >}}

#### FlatMap

{{< label Batch >}} {{< label Streaming >}}

{{< tabs "flatmap" >}}
{{< tab "Java" >}}

使用表函数执行 `flatMap` 操作。

```java
public class MyFlatMapFunction extends TableFunction<Row> {

    public void eval(String str) {
        if (str.contains("#")) {
            String[] array = str.split("#");
            for (int i = 0; i < array.length; ++i) {
                collect(Row.of(array[i], array[i].length()));
            }
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }
}

TableFunction func = new MyFlatMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .flatMap(call("func", $("c")).as("a", "b"));
```
{{< /tab >}}
{{< tab "Scala" >}}

使用表函数执行 `flatMap` 操作。

```scala
class MyFlatMapFunction extends TableFunction[Row] {
  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}

val func = new MyFlatMapFunction
val table = input
  .flatMap(func($"c")).as("a", "b")
```
{{< /tab >}}
{{< tab "Python" >}}

通过 python [表函数]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-functions)执行 `flat_map` 操作。

```python
from pyflink.table.udf import udtf
from pyflink.table import DataTypes
from pyflink.common import Row

@udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
def split(x: Row) -> Row:
    for s in x.b.split(","):
        yield x.a, s

input.flat_map(split)
```
{{< /tab >}}
{{< /tabs >}}

#### Aggregate

{{< label Batch >}} {{< label Streaming >}} {{< label Result Updating >}}

{{< tabs "aggregate" >}}
{{< tab "Java" >}}

使用聚合函数来执行聚合操作。你必须使用 select 子句关闭 `aggregate`，并且 select 子句不支持聚合函数。如果输出类型是复合类型，则聚合的输出将被展平。

```java
public class MyMinMaxAcc {
    public int min = 0;
    public int max = 0;
}

public class MyMinMax extends AggregateFunction<Row, MyMinMaxAcc> {

    public void accumulate(MyMinMaxAcc acc, int value) {
        if (value < acc.min) {
            acc.min = value;
        }
        if (value > acc.max) {
            acc.max = value;
        }
    }

    @Override
    public MyMinMaxAcc createAccumulator() {
        return new MyMinMaxAcc();
    }

    public void resetAccumulator(MyMinMaxAcc acc) {
        acc.min = 0;
        acc.max = 0;
    }

    @Override
    public Row getValue(MyMinMaxAcc acc) {
        return Row.of(acc.min, acc.max);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.INT, Types.INT);
    }
}

AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);
Table table = input
  .groupBy($("key"))
  .aggregate(call("myAggFunc", $("a")).as("x", "y"))
  .select($("key"), $("x"), $("y"));
```
{{< /tab >}}
{{< tab "Scala" >}}

使用聚合函数来执行聚合操作。你必须使用 select 子句关闭 `aggregate`，并且 select 子句不支持聚合函数。如果输出类型是复合类型，则聚合的输出将被展平。

```scala
case class MyMinMaxAcc(var min: Int, var max: Int)

class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

  def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
    if (value < acc.min) {
      acc.min = value
    }
    if (value > acc.max) {
      acc.max = value
    }
  }

  override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

  def resetAccumulator(acc: MyMinMaxAcc): Unit = {
    acc.min = 0
    acc.max = 0
  }

  override def getValue(acc: MyMinMaxAcc): Row = {
    Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.INT, Types.INT)
  }
}

val myAggFunc = new MyMinMax
val table = input
  .groupBy($"key")
  .aggregate(myAggFunc($"a") as ("x", "y"))
  .select($"key", $"x", $"y")
```
{{< /tab >}}
{{< tab "Python" >}}

使用 python 的[通用聚合函数]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#aggregate-functions)或 [向量化聚合函数]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-aggregate-functions)来执行聚合操作。你必须使用 select 子句关闭 `aggregate` ，并且 select 子句不支持聚合函数。如果输出类型是复合类型，则聚合的输出将被展平。

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf

class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, row: Row):
        accumulator[0] += 1
        accumulator[1] += row.b

    def retract(self, accumulator, row: Row):
        accumulator[0] -= 1
        accumulator[1] -= row.b

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

function = CountAndSumAggregateFunction()
agg = udaf(function,
           result_type=function.get_result_type(),
           accumulator_type=function.get_accumulator_type(),
           name=str(function.__class__.__name__))

# 使用 python 通用聚合函数进行聚合
result = t.group_by(t.a) \
    .aggregate(agg.alias("c", "d")) \
    .select("a, c, d")
    
# 使用 python 向量化聚合函数进行聚合
pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
t.aggregate(pandas_udaf.alias("a", "b")) \
    .select("a, b")

```

{{< /tab >}}
{{< /tabs >}}

####  Group Window Aggregate

{{< label Batch >}} {{< label Streaming >}}

在 [group window](#group-window) 和可能的一个或多个分组键上对表进行分组和聚合。你必须使用 select 子句关闭 `aggregate`。并且 select 子句不支持“*"或聚合函数。

{{< tabs "group-window-agg" >}}
{{< tab "Java" >}}
```java
AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);

Table table = input
    .window(Tumble.over(lit(5).minutes())
                  .on($("rowtime"))
                  .as("w")) // 定义窗口
    .groupBy($("key"), $("w")) // 以键和窗口分组
    .aggregate(call("myAggFunc", $("a")).as("x", "y"))
    .select($("key"), $("x"), $("y"), $("w").start(), $("w").end()); // 访问窗口属性与聚合结果
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val myAggFunc = new MyMinMax
val table = input
    .window(Tumble over 5.minutes on $"rowtime" as "w") // 定义窗口
    .groupBy($"key", $"w") // 以键和窗口分组
    .aggregate(myAggFunc($"a") as ("x", "y"))
    .select($"key", $"x", $"y", $"w".start, $"w".end) // 访问窗口属性与聚合结果
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf

pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
tumble_window = Tumble.over(expr.lit(1).hours) \
    .on(expr.col("rowtime")) \
    .alias("w")
t.select(t.b, t.rowtime) \
    .window(tumble_window) \
    .group_by("w") \
    .aggregate(pandas_udaf.alias("d", "e")) \
    .select("w.rowtime, d, e")
```
{{< /tab >}}
{{< /tabs >}}

#### FlatAggregate

{{< tabs "flataggregate" >}}
{{< tab "Java" >}}

和 **GroupBy Aggregation** 类似。使用运行中的表之后的聚合算子对分组键上的行进行分组，以按组聚合行。和 AggregateFunction 的不同之处在于，TableAggregateFunction 的每个分组可能返回0或多条记录。你必须使用 select 子句关闭 `flatAggregate`。并且 select 子句不支持聚合函数。

除了使用 emitValue 输出结果，你还可以使用 emitUpdateWithRetract 方法。和 emitValue 不同的是，emitUpdateWithRetract 用于下发已更新的值。此方法在retract 模式下增量输出数据，例如，一旦有更新，我们必须在发送新的更新记录之前收回旧记录。如果在表聚合函数中定义了这两个方法，则将优先使用 emitUpdateWithRetract 方法而不是 emitValue 方法，这是因为该方法可以增量输出值，因此被视为比 emitValue 方法更有效。

```java
/**
 * Top2 Accumulator。
 */
public class Top2Accum {
    public Integer first;
    public Integer second;
}

/**
 * 用户定义的聚合函数 top2。
 */
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }


    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
        for (Top2Accum otherAcc : iterable) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }

    public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
        // 下发 value 与 rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}

tEnv.registerFunction("top2", new Top2());
Table orders = tableEnv.from("Orders");
Table result = orders
    .groupBy($("key"))
    .flatAggregate(call("top2", $("a")).as("v", "rank"))
    .select($("key"), $("v"), $("rank");
```
{{< /tab >}}
{{< tab "Scala" >}}

和 **GroupBy Aggregation** 类似。使用运行中的表之后的聚合运算符对分组键上的行进行分组，以按组聚合行。和 AggregateFunction 的不同之处在于，TableAggregateFunction 的每个分组可能返回0或多条记录。你必须使用 select 子句关闭 `flatAggregate`。并且 select 子句不支持聚合函数。

除了使用 emitValue 输出结果，你还可以使用 emitUpdateWithRetract 方法。和 emitValue 不同的是，emitUpdateWithRetract 用于发出已更新的值。此方法在retract 模式下增量输出数据，例如，一旦有更新，我们必须在发送新的更新记录之前收回旧记录。如果在表聚合函数中定义了这两个方法，则将优先使用 emitUpdateWithRetract 方法而不是 emitValue 方法，这是因为该方法可以增量输出值，因此被视为比 emitValue 方法更有效。

```scala
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Top2 Accumulator。
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * 用户定义的聚合函数 top2。
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // 下发 value 与 rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}

val top2 = new Top2
val orders: Table = tableEnv.from("Orders")
val result = orders
    .groupBy($"key")
    .flatAggregate(top2($"a") as ($"v", $"rank"))
    .select($"key", $"v", $"rank")
```
{{< /tab >}}
{{< tab "Python" >}}

使用 python 通用 [Table Aggregate Function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-aggregate-functions) 执行 flat_aggregate 操作。

和 **GroupBy Aggregation** 类似。使用运行中的表之后的聚合运算符对分组键上的行进行分组，以按组聚合行。和 AggregateFunction 的不同之处在于，TableAggregateFunction 的每个分组可能返回0或多条记录。你必须使用 select 子句关闭 `flat_aggregate`。并且 select 子句不支持聚合函数。

```python
from pyflink.common import Row
from pyflink.table.udf import TableAggregateFunction, udtaf
from pyflink.table import DataTypes

class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, row: Row):
        if row.a is not None:
            if accumulator[0] is None or row.a > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = row.a
            elif accumulator[1] is None or row.a > accumulator[1]:
                accumulator[1] = row.a

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            self.accumulate(accumulator, other_acc[0])
            self.accumulate(accumulator, other_acc[1])

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])

mytop = udtaf(Top2())
t = t_env.from_elements([(1, 'Hi', 'Hello'),
                              (3, 'Hi', 'hi'),
                              (5, 'Hi2', 'hi'),
                              (7, 'Hi', 'Hello'),
                              (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
result = t.select(t.a, t.c) \
    .group_by(t.c) \
    .flat_aggregate(mytop) \
    .select(t.a) \
    .flat_aggregate(mytop.alias("b"))

```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

<a name="data-types"></a>
数据类型
----------

请查看[数据类型]({{< ref "docs/dev/table/types" >}})的专门页面。

行中的字段可以是一般类型和(嵌套)复合类型(比如 POJO、元组、行、 Scala 案例类)。

任意嵌套的复合类型的字段都可以通过[值访问函数]({{< ref "docs/dev/table/functions/systemFunctions" >}}#value-access-functions)来访问。

[用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})可以将泛型当作黑匣子一样传输和处理。

{{< top >}}

