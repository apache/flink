---
title: "CREATE 语句"
nav-parent_id: sql
nav-pos: 2
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

CREATE 语句用于向当前或指定的 [Catalog]({{ site.baseurl }}/zh/dev/table/catalogs.html) 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。

目前 Flink SQL 支持下列 CREATE 语句：

- CREATE TABLE
- CREATE DATABASE
- CREATE FUNCTION

## 执行 CREATE 语句

可以使用 `TableEnvironment` 中的 `sqlUpdate()` 方法执行 CREATE 语句，也可以在 [SQL CLI]({{ site.baseurl }}/zh/dev/table/sqlClient.html) 中执行 CREATE 语句。 若 CREATE 操作执行成功，`sqlUpdate()` 方法不返回任何内容，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 和  SQL CLI 中执行一个 CREATE 语句。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// 对已经已经注册的表进行 SQL 查询
// 注册名为 “Orders” 的表
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// 在表上执行 SQL 查询，并把得到的结果作为一个新的表
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL 对已注册的表进行 update 操作
// 注册 TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)");
// 在表上执行 SQL 更新查询并向 TableSink 发出结果
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// 对已经已经注册的表进行 SQL 查询
// 注册名为 “Orders” 的表
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// 在表上执行 SQL 查询，并把得到的结果作为一个新的表
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL 对已注册的表进行 update 操作
// 注册 TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH ('connector.path'='/path/to/file' ...)");
// 在表上执行 SQL 更新查询并向 TableSink 发出结果
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# 对已经已经注册的表进行 SQL 查询
# 注册名为 “Orders” 的表
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
# 在表上执行 SQL 查询，并把得到的结果作为一个新的表
result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

# SQL 对已注册的表进行 update 操作
# 注册 TableSink
table_env.sql_update("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")
# 在表上执行 SQL 更新查询并向 TableSink 发出结果
table_env \
    .sql_update("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders (product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
{% endhighlight %}
</div>
</div>

{% top %}

##  CREATE TABLE

{% highlight sql %}
CREATE TABLE [catalog_name.][db_name.]table_name
  (
    { <column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)

<column_definition>:
  column_name column_type [COMMENT column_comment]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

{% endhighlight %}

根据指定的表名创建一个表，如果同名表已经在 catalog 中存在了，则无法注册。

**COMPUTED COLUMN**

计算列是一个使用 “`column_name AS computed_column_expression`” 语法生成的虚拟列。它由使用同一表中其他列的非查询表达式生成，并且不会在表中进行物理存储。例如，一个计算列可以使用 `cost AS price * quantity` 进行定义，这个表达式可以包含物理列、常量、函数或变量的任意组合，但这个表达式不能存在任何子查询。

在 Flink 中计算列一般用于为 CREATE TABLE 语句定义 [时间属性]({{ site.baseurl}}/zh/dev/table/streaming/time_attributes.html)。
[处理时间属性]({{ site.baseurl}}/zh/dev/table/streaming/time_attributes.html#processing-time) 可以简单地通过使用了系统函数 `PROCTIME()` 的 `proc AS PROCTIME()` 语句进行定义。
另一方面，由于事件时间列可能需要从现有的字段中获得，因此计算列可用于获得事件时间列。例如，原始字段的类型不是 `TIMESTAMP(3)` 或嵌套在 JSON 字符串中。

注意：

- 定义在一个数据源表（ source table ）上的计算列会在从数据源读取数据后被计算，它们可以在 SELECT 查询语句中使用。
- 计算列不可以作为 INSERT 语句的目标，在 INSERT 语句中，SELECT 语句的 schema 需要与目标表不带有计算列的 schema 一致。

**WATERMARK**

`WATERMARK` 定义了表的事件时间属性，其形式为 `WATERMARK FOR rowtime_column_name  AS watermark_strategy_expression` 。

`rowtime_column_name` 把一个现有的列定义为一个为表标记事件时间的属性。该列的类型必须为 `TIMESTAMP(3)`，且是 schema 中的顶层列，它也可以是一个计算列。

`watermark_strategy_expression` 定义了 watermark 的生成策略。它允许使用包括计算列在内的任意非查询表达式来计算 watermark ；表达式的返回类型必须是 `TIMESTAMP(3)`，表示了从 Epoch 以来的经过的时间。
返回的 watermark 只有当其不为空且其值大于之前发出的本地 watermark 时才会被发出（以保证 watermark 递增）。每条记录的 watermark 生成表达式计算都会由框架完成。
框架会定期发出所生成的最大的 watermark ，如果当前 watermark 仍然与前一个 watermark 相同、为空、或返回的 watermark 的值小于最后一个发出的 watermark ，则新的 watermark 不会被发出。
Watermark 根据 [`pipeline.auto-watermark-interval`]({{ site.baseurl }}/zh/ops/config.html#pipeline-auto-watermark-interval) 中所配置的间隔发出。
若 watermark 的间隔是 `0ms` ，那么每条记录都会产生一个 watermark，且 watermark 会在不为空并大于上一个发出的 watermark 时发出。

使用事件时间语义时，表必须包含事件时间属性和 watermark 策略。

Flink 提供了几种常用的 watermark 策略。

- 严格递增时间戳： `WATERMARK FOR rowtime_column AS rowtime_column`。

  发出到目前为止已观察到的最大时间戳的 watermark ，时间戳小于最大时间戳的行被认为没有迟到。

- 递增时间戳： `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND`。

  发出到目前为止已观察到的最大时间戳减 1 的 watermark ，时间戳等于或小于最大时间戳的行被认为没有迟到。

- 有界乱序时间戳： `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit`。

  发出到目前为止已观察到的最大时间戳减去指定延迟的 watermark ，例如， `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND` 是一个 5 秒延迟的 watermark 策略。

{% highlight sql %}
CREATE TABLE Orders (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ( . . . );
{% endhighlight %}

**PARTITIONED BY**

根据指定的列对已经创建的表进行分区。若表使用 filesystem sink ，则将会为每个分区创建一个目录。

**WITH OPTIONS**

表属性用于创建 table source/sink ，一般用于寻找和创建底层的连接器。

表达式 `key1=val1` 的键和值必须为字符串文本常量。请参考 [连接外部系统]({{ site.baseurl }}/zh/dev/table/connect.html) 了解不同连接器所支持的属性。

**注意：** 表名可以为以下三种格式 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`。使用`catalog_name.db_name.table_name` 的表将会与名为 "catalog_name" 的 catalog 和名为 "db_name" 的数据库一起注册到 metastore 中。使用 `db_name.table_name` 的表将会被注册到当前执行的 table environment 中的 catalog 且数据库会被命名为 "db_name"；对于 `table_name`, 数据表将会被注册到当前正在运行的catalog和数据库中。

**注意：** 使用 `CREATE TABLE` 语句注册的表均可用作 table source 和 table sink。 在被 DML 语句引用前，我们无法决定其实际用于 source 抑或是 sink。

**LIKE clause**

The `LIKE` clause is a variant/combination of SQL features (Feature T171, “LIKE clause in table definition” and Feature T173, “Extended LIKE clause in table definition”). The clause can be used to create a table based on a definition of an existing table. Additionally, users
can extend the original table or exclude certain parts of it. In contrast to the SQL standard the clause must be defined at the top-level of a CREATE statement. That is because the clause applies to multiple parts of the definition and not only to the schema part.

You can use the clause to reuse (and potentially overwrite) certain connector properties or add watermarks to tables defined externally. For example, you can add a watermark to a table defined in Apache Hive. 

Consider the example statement below:
{% highlight sql %}
CREATE TABLE Orders (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3)
) WITH ( 
    'connector' = 'kafka',
    'startup-mode' = 'earliest-offset'
);

CREATE TABLE Orders_with_watermark (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    -- Overwrite the startup-mode
    'startup-mode' = 'latest-offset'
)
LIKE Orders;
{% endhighlight %}

The resulting table `Orders_with_watermark` will be equivalent to a table created with a following statement:
{% highlight sql %}
CREATE TABLE Orders_with_watermark (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    'startup-mode' = 'latest-offset'
);
{% endhighlight %}

The merging logic of table features can be controlled with `like options`.

You can control the merging behavior of:

* CONSTRAINTS - constraints such as primary and unique keys
* GENERATED - computed columns
* OPTIONS - connector options that describe connector and format properties
* PARTITIONS - partition of the tables
* WATERMARKS - watermark declarations

with three different merging strategies:

* INCLUDING - Includes the feature of the source table, fails on duplicate entries, e.g. if an option with the same key exists in both tables.
* EXCLUDING - Does not include the given feature of the source table.
* OVERWRITING - Includes the feature of the source table, overwrites duplicate entries of the source table with properties of the new table, e.g. if an option with the same key exists in both tables, the one from the current statement will be used.

Additionally, you can use the `INCLUDING/EXCLUDING ALL` option to specify what should be the strategy if there was no specific strategy defined, i.e. if you use `EXCLUDING ALL INCLUDING WATERMARKS` only the watermarks will be included from the source table.

Example:
{% highlight sql %}
-- A source table stored in a filesystem
CREATE TABLE Orders_in_file (
    user BIGINT,
    product STRING,
    order_time_string STRING,
    order_time AS to_timestamp(order_time)
    
)
PARTITIONED BY user 
WITH ( 
    'connector' = 'filesystem'
    'path' = '...'
);

-- A corresponding table we want to store in kafka
CREATE TABLE Orders_in_kafka (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector': 'kafka'
    ...
)
LIKE Orders_in_file (
    -- Exclude everything besides the computed columns which we need to generate the watermark for.
    -- We do not want to have the partitions or filesystem options as those do not apply to kafka. 
    EXCLUDING ALL
    INCLUDING GENERATED
);
{% endhighlight %}

If you provide no like options, `INCLUDING ALL OVERWRITING OPTIONS` will be used as a default.

**NOTE** You cannot control the behavior of merging physical fields. Those will be merged as if you applied the `INCLUDING` strategy.

{% top %}

## CREATE CATALOG

{% highlight sql %}
CREATE CATALOG catalog_name
  WITH (key1=val1, key2=val2, ...)
{% endhighlight %}

Create a catalog with the given catalog properties. If a catalog with the same name already exists, an exception is thrown.

**WITH OPTIONS**

Catalog properties used to store extra information related to this catalog.
The key and value of expression `key1=val1` should both be string literal.

Check out more details at [Catalogs]({{ site.baseurl }}/dev/table/catalogs.html).

{% top %}

## CREATE DATABASE

{% highlight sql %}
CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...)
{% endhighlight %}

根据给定的表属性创建数据库。若数据库中已存在同名表会抛出异常。

**IF NOT EXISTS**

若数据库已经存在，则不会进行任何操作。

**WITH OPTIONS**

数据库属性一般用于存储关于这个数据库额外的信息。
表达式 `key1=val1` 中的键和值都需要是字符串文本常量。

{% top %}

## CREATE FUNCTION
{% highlight sql%}
CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION
  [IF NOT EXISTS] [[catalog_name.]db_name.]function_name
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
{% endhighlight %}

创建一个有 catalog 和数据库命名空间的 catalog function ，需要指定一个 identifier ，可指定 language tag 。 若 catalog 中，已经有同名的函数注册了，则无法注册。

如果 language tag 是 JAVA 或者 SCALA ，则 identifier 是 UDF 实现类的全限定名。关于 JAVA/SCALA UDF 的实现，请参考 [自定义函数]({{ site.baseurl }}/zh/dev/table/functions/udfs.html)。

如果 language tag 是 PYTHON ，则 identifier 是 UDF 对象的全限定名，例如 `pyflink.table.tests.test_udf.add`。关于 PYTHON UDF 的实现，请参考 [Python UDFs]({{ site.baseurl }}/zh/dev/table/python/python_udfs.html)。

**TEMPORARY**

创建一个有 catalog 和数据库命名空间的临时 catalog function ，并覆盖原有的 catalog function 。

**TEMPORARY SYSTEM**

创建一个没有数据库命名空间的临时系统 catalog function ，并覆盖系统内置的函数。

**IF NOT EXISTS**

若该函数已经存在，则不会进行任何操作。

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag 用于指定 Flink runtime 如何执行这个函数。目前，只支持 JAVA, SCALA 和 PYTHON，且函数的默认语言为 JAVA。

