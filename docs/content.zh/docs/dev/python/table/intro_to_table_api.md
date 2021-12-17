---
title: "Python Table API 简介"
weight: 26
type: docs
aliases:
  - /zh/dev/python/table-api-users-guide/intro_to_table_api.html
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

# Python Table API 简介

本文档是对 PyFlink Table API 的简要介绍，用于帮助新手用户快速理解 PyFlink Table API 的基本用法。
关于高级用法，请参阅用户指南中的其他文档。



Python Table API 程序的基本结构 
--------------------------------------------

所有的 Table API 和 SQL 程序，不管批模式，还是流模式，都遵循相同的结构。下面代码示例展示了 Table API 和 SQL 程序的基本结构。

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. 创建 TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings) 

# 2. 创建 source 表
table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '10'
    )
""")

# 3. 创建 sink 表
table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'print'
    )
""")

# 4. 查询 source 表，同时执行计算
# 通过 Table API 创建一张表：
source_table = table_env.from_path("datagen")
# 或者通过 SQL 查询语句创建一张表：
source_table = table_env.sql_query("SELECT * FROM datagen")

result_table = source_table.select(source_table.id + 1, source_table.data)

# 5. 将计算结果写入给 sink 表
# 将 Table API 结果表数据写入 sink 表：
result_table.execute_insert("print").wait()
# 或者通过 SQL 查询语句来写入 sink 表：
table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()
```

{{< top >}}

创建 TableEnvironment
---------------------------

`TableEnvironment` 是 Table API 和 SQL 集成的核心概念。下面代码示例展示了如何创建一个 `TableEnvironment`:

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# or create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)
```

关于创建 `TableEnvironment` 的更多细节，请查阅 [TableEnvironment 文档]({{< ref "docs/dev/python/table/table_environment" >}}#create-a-tableenvironment)。

`TableEnvironment` 可以用来:

* 创建 `Table`
* 将 `Table` 注册成临时表
* 执行 SQL 查询，更多细节可查阅 [SQL]({{< ref "docs/dev/table/sql/overview" >}})
* 注册用户自定义的 (标量，表值，或者聚合) 函数, 更多细节可查阅 [普通的用户自定义函数]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}) 和 [向量化的用户自定义函数]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}})
* 配置作业，更多细节可查阅 [Python 配置]({{< ref "docs/dev/python/python_config" >}})
* 管理 Python 依赖，更多细节可查阅 [依赖管理]({{< ref "docs/dev/python/dependency_management" >}})
* 提交作业执行

{{< top >}}

创建表
---------------

`Table` 是 Python Table API 的核心组件。`Table` 是 Table API 作业中间结果的逻辑表示。

一个 `Table` 实例总是与一个特定的 `TableEnvironment` 相绑定。不支持在同一个查询中合并来自不同 TableEnvironments 的表，例如 join 或者 union 它们。

### 通过列表类型的对象创建

你可以使用一个列表对象创建一张表：

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 创建 批 TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])
table.to_pandas()
```

结果为：

```text
   _1     _2
0   1     Hi
1   2  Hello
```

你也可以创建具有指定列名的表：

```python
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.to_pandas()
```

结果为：

```text
   id   data
0   1     Hi
1   2  Hello
```

默认情况下，表结构是从数据中自动提取的。

如果自动生成的表模式不符合你的要求，你也可以手动指定：

```python
table_without_schema = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
# 默认情况下，“id” 列的类型是 64 位整型
default_type = table_without_schema.to_pandas()["id"].dtype
print('By default the type of the "id" column is %s.' % default_type)

from pyflink.table import DataTypes
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                               DataTypes.FIELD("data", DataTypes.STRING())]))
# 现在 “id” 列的类型是 8 位整型
type = table.to_pandas()["id"].dtype
print('Now the type of the "id" column is %s.' % type)
```

结果为：

```text
默认情况下，“id” 列的类型是 64 位整型。
现在 “id” 列的类型是 8 位整型。
```

### 通过 DDL 创建

你可以通过 DDL 创建一张表：

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 创建 流 TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql("""
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='3',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='6'
    )
""")
table = table_env.from_path("random_source")
table.to_pandas()
```

结果为：

```text
   id  data
0   2     5
1   1     4
2   3     6
```

### 通过 Catalog 创建

`TableEnvironment` 维护了一个使用标识符创建的表的 catalogs 映射。

Catalog 中的表既可以是临时的，并与单个 Flink 会话生命周期相关联，也可以是永久的，跨多个 Flink 会话可见。

通过 SQL DDL 创建的表和视图， 例如 "create table ..." 和 "create view ..."，都存储在 catalog 中。

你可以通过 SQL 直接访问 catalog 中的表。

如果你要用 Table API 来使用 catalog 中的表，可以使用 "from_path" 方法来创建 Table API 对象：

```python
# 准备 catalog
# 将 Table API 表注册到 catalog 中
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('source_table', table)

# 从 catalog 中获取 Table API 表
new_table = table_env.from_path('source_table')
new_table.to_pandas()
```

结果为：

```text
   id   data
0   1     Hi
1   2  Hello
```

{{< top >}}

查询
---------------

### Table API 查询

`Table` 对象有许多方法，可以用于进行关系操作。
这些方法返回新的 `Table` 对象，表示对输入 `Table` 应用关系操作之后的结果。
这些关系操作可以由多个方法调用组成，例如 `table.group_by(...).select(...)`。

[Table API]({{< ref "docs/dev/table/tableApi" >}}?code_tab=python) 文档描述了流和批处理上所有支持的 Table API 操作。

以下示例展示了一个简单的 Table API 聚合查询：

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 通过 batch table environment 来执行查询
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                 ['name', 'country', 'revenue'])

# 计算所有来自法国客户的收入
revenue = orders \
    .select(orders.name, orders.country, orders.revenue) \
    .where(orders.country == 'FRANCE') \
    .group_by(orders.name) \
    .select(orders.name, orders.revenue.sum.alias('rev_sum'))
    
revenue.to_pandas()
```

结果为：

```text
   name  rev_sum
0  Jack       30
```

Table API 也支持 [行操作]({{< ref "docs/dev/table/tableapi" >}}#row-based-operations)的 API, 这些行操作包括 [Map Operation]({{< ref "docs/dev/table/tableapi" >}}#row-based-operations), 
[FlatMap Operation]({{< ref "docs/dev/table/tableapi" >}}#flatmap), [Aggregate Operation]({{< ref "docs/dev/table/tableapi" >}}#aggregate) 和 [FlatAggregate Operation]({{< ref "docs/dev/table/tableapi" >}}#flataggregate).

以下示例展示了一个简单的 Table API 基于行操作的查询

```python
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.table.udf import udf
import pandas as pd

# 通过 batch table environment 来执行查询
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                 ['name', 'country', 'revenue'])

map_function = udf(lambda x: pd.concat([x.name, x.revenue * 10], axis=1),
                    result_type=DataTypes.ROW(
                                [DataTypes.FIELD("name", DataTypes.STRING()),
                                 DataTypes.FIELD("revenue", DataTypes.BIGINT())]),
                    func_type="pandas")

orders.map(map_function).alias('name', 'revenue').to_pandas()
```

结果为：

```text
   name  revenue
0  Jack      100
1  Rose      300
2  Jack      200
```

### SQL 查询

Flink 的 SQL 基于 [Apache Calcite](https://calcite.apache.org)，它实现了标准的 SQL。SQL 查询语句使用字符串来表达。

[SQL]({{< ref "docs/dev/table/sql/overview" >}}) 文档描述了 Flink 对流和批处理所支持的 SQL。

下面示例展示了一个简单的 SQL 聚合查询：

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# 通过 stream table environment 来执行查询
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


table_env.execute_sql("""
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='8',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='11'
    )
""")

table_env.execute_sql("""
    CREATE TABLE print_sink (
        id BIGINT, 
        data_sum TINYINT 
    ) WITH (
        'connector' = 'print'
    )
""")

table_env.execute_sql("""
    INSERT INTO print_sink
        SELECT id, sum(data) as data_sum FROM 
            (SELECT id / 2 as id, data FROM random_source)
        WHERE id > 1
        GROUP BY id
""").wait()
```

结果为：

```text
2> +I(4,11)
6> +I(2,8)
8> +I(3,10)
6> -U(2,8)
8> -U(3,10)
6> +U(2,15)
8> +U(3,19)
```

实际上，上述输出展示了 print 结果表所接收到的 change log。
change log 的格式为:
```text
{subtask id}> {消息类型}{值的字符串格式}
```
例如，"2> +I(4,11)" 表示这条消息来自第二个 subtask，其中 "+I" 表示这是一条插入的消息，"(4, 11)" 是这条消息的内容。
另外，"-U" 表示这是一条撤回消息 (即更新前)，这意味着应该在 sink 中删除或撤回该消息。 
"+U" 表示这是一条更新的记录 (即更新后)，这意味着应该在 sink 中更新或插入该消息。

所以，从上面的 change log，我们可以得到如下结果：

```text
(4, 11)
(2, 15) 
(3, 19)
```

### Table API 和 SQL 的混合使用

Table API 中的 `Table` 对象和 SQL 中的 Table 可以自由地相互转换。

下面例子展示了如何在 SQL 中使用 `Table` 对象：

```python
# 创建一张 sink 表来接收结果数据
table_env.execute_sql("""
    CREATE TABLE table_sink (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")

# 将 Table API 表转换成 SQL 中的视图
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('table_api_table', table)

# 将 Table API 表的数据写入结果表
table_env.execute_sql("INSERT INTO table_sink SELECT * FROM table_api_table").wait()
```

结果为：

```text
6> +I(1,Hi)
6> +I(2,Hello)
```

下面例子展示了如何在 Table API 中使用 SQL 表：

```python
# 创建一张 SQL source 表
table_env.execute_sql("""
    CREATE TABLE sql_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='4',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='7'
    )
""")

# 将 SQL 表转换成 Table API 表
table = table_env.from_path("sql_source")

# 或者通过 SQL 查询语句创建表
table = table_env.sql_query("SELECT * FROM sql_source")

# 将表中的数据写出
table.to_pandas()
```

结果为：

```text
   id  data
0   2     5
1   1     4
2   4     7
3   3     6
```

{{< top >}}

将结果写出
----------------

### 将结果数据收集到客户端

你可以使用 `TableResult.collect` 将 Table 的结果收集到客户端，结果的类型为迭代器类型。

以下代码展示了如何使用 `TableResult.collect()` 方法：

```python
# 准备 source 表
source = table_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])

# 得到 TableResult
res = table_env.execute_sql("select a + 1, b, c from %s" % source)

# 遍历结果
with res.collect() as results:
   for result in results:
       print(result)
```

结果为：

```text
<Row(2, 'Hi', 'Hello')>
<Row(3, 'Hello', 'Hello')>
```

### 将结果数据转换为Pandas DataFrame，并收集到客户端

你可以调用 "to_pandas" 方法来 [将一个 `Table` 对象转化成 pandas DataFrame]({{< ref "docs/dev/python/table/conversion_of_pandas" >}}#convert-pyflink-table-to-pandas-dataframe):

```python
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.to_pandas()
```

结果为：

```text
   id   data
0   1     Hi
1   2  Hello
```

<span class="label label-info">Note</span> "to_pandas" 会触发表的物化，同时将表的内容收集到客户端内存中，所以通过 {{< pythondoc file="pyflink.table.html#pyflink.table.Table.limit" name="Table.limit">}} 来限制收集数据的条数是一种很好的做法。

<span class="label label-info">Note</span> flink planner 不支持 "to_pandas"，并且，并不是所有的数据类型都可以转换为 pandas DataFrames。

### 将结果写入到一张 Sink 表中

你可以调用 "execute_insert" 方法来将 `Table` 对象中的数据写入到一张 sink 表中：

```python
table_env.execute_sql("""
    CREATE TABLE sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table.execute_insert("sink_table").wait()
```

结果为：

```text
6> +I(1,Hi)
6> +I(2,Hello)
```

也可以通过 SQL 来完成:

```python
table_env.create_temporary_view("table_source", table)
table_env.execute_sql("INSERT INTO sink_table SELECT * FROM table_source").wait()
```

### 将结果写入多张 Sink 表中

你也可以使用 `StatementSet` 在一个作业中将 `Table` 中的数据写入到多张 sink 表中：

```python
# 准备 source 表和 sink 表
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view("simple_source", table)
table_env.execute_sql("""
    CREATE TABLE first_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE second_sink_table (
        id BIGINT, 
        data VARCHAR
    ) WITH (
        'connector' = 'print'
    )
""")

# 创建 statement set
statement_set = table_env.create_statement_set()

# 将 "table" 的数据写入 "first_sink_table"
statement_set.add_insert("first_sink_table", table)

# 通过一条 sql 插入语句将数据从 "simple_source" 写入到 "second_sink_table"
statement_set.add_insert_sql("INSERT INTO second_sink_table SELECT * FROM simple_source")

# 执行 statement set
statement_set.execute().wait()
```

结果为：

```text
7> +I(1,Hi)
7> +I(1,Hi)
7> +I(2,Hello)
7> +I(2,Hello)
```

Explain 表
-----------------

Table API 提供了一种机制来查看 `Table` 的逻辑查询计划和优化后的查询计划。 
这是通过 `Table.explain()` 或者 `StatementSet.explain()` 方法来完成的。`Table.explain()` 可以返回一个 `Table` 的执行计划。`StatementSet.explain()` 则可以返回含有多个 sink 的作业的执行计划。这些方法会返回一个字符串，字符串描述了以下三个方面的信息：

1. 关系查询的抽象语法树，即未经优化的逻辑查询计划，
2. 优化后的逻辑查询计划，
3. 物理执行计划。

`TableEnvironment.explain_sql()` 和 `TableEnvironment.execute_sql()` 支持执行 `EXPLAIN` 语句获得执行计划。更多细节请查阅 [EXPLAIN]({{< ref "docs/dev/table/sql/explain" >}})。

以下代码展示了如何使用 `Table.explain()` 方法：

```python
# 使用流模式 TableEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table = table1 \
    .where(table1.data.like('H%')) \
    .union_all(table2)
print(table.explain())
```

结果为：

```text
== 抽象语法树 ==
LogicalUnion(all=[true])
:- LogicalFilter(condition=[LIKE($1, _UTF-16LE'H%')])
:  +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]]])
+- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]]])

== 优化后的逻辑计划 ==
Union(all=[true], union=[id, data])
:- Calc(select=[id, data], where=[LIKE(data, _UTF-16LE'H%')])
:  +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

== 物理执行计划 ==
Stage 133 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 134 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_201907291, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 135 : Operator
                        content : Calc(select=[id, data], where=[(data LIKE _UTF-16LE'H%')])
                        ship_strategy : FORWARD

Stage 136 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 137 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_1709623525, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

```

以下代码展示了如何使用 `StatementSet.explain()` 方法：

```python
# 使用流模式 TableEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.execute_sql("""
    CREATE TABLE print_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE black_hole_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'blackhole'
    )
""")

statement_set = table_env.create_statement_set()

statement_set.add_insert("print_sink_table", table1.where(table1.data.like('H%')))
statement_set.add_insert("black_hole_sink_table", table2)

print(statement_set.explain())
```

结果为

```text
== 抽象语法树 ==
LogicalSink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'H%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]]])

LogicalSink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
+- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]]])

== 优化后的逻辑计划 ==
Sink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
+- Calc(select=[id, data], where=[LIKE(data, _UTF-16LE'H%')])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

Sink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])

== 物理执行计划 ==
Stage 139 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 140 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_541737614, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 141 : Operator
                        content : Calc(select=[id, data], where=[(data LIKE _UTF-16LE'H%')])
                        ship_strategy : FORWARD

Stage 143 : Data Source
        content : Source: PythonInputFormatTableSource(id, data)

        Stage 144 : Operator
                content : SourceConversion(table=[default_catalog.default_database.Unregistered_TableSource_1437429083, source: [PythonInputFormatTableSource(id, data)]], fields=[id, data])
                ship_strategy : FORWARD

                Stage 142 : Data Sink
                        content : Sink: Sink(table=[default_catalog.default_database.print_sink_table], fields=[id, data])
                        ship_strategy : FORWARD

                        Stage 145 : Data Sink
                                content : Sink: Sink(table=[default_catalog.default_database.black_hole_sink_table], fields=[id, data])
                                ship_strategy : FORWARD
```
