---
title: '基于 Table API 实现实时报表'
nav-title: '基于 Table API 实现实时报表'
weight: 4
type: docs
aliases:
  - /zh/try-flink/table_api.html
  - /zh/getting-started/walkthroughs/table_api.html
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

# 基于 Table API 实现实时报表

Apache Flink提供了一个Table API，作为用于批处理和流处理的统一的关系API，即在无界实时流或有界批数据集中以相同的语义执行查询，并产生相同的结果。
Flink中的Table API通常用于简化数据分析，数据流水线和ETL应用程序的定义。

## 你将构建什么？

在本教程中，您将学习如何构建实时仪表板，以按帐户跟踪金融交易。
数据管道将从Kafka读取数据，并通过Grafana将结果写入到可视化的MySQL。

## 先决条件


本教程已默认您对Java或Scala有一定的了解，但是即使您使用不同的编程语言，亦可遵循此教程。
我们还假定您熟悉基本的关系概念，例如`SELECT`和`GROUP BY`子句。

## 求助，我遇到问题了！

如果遇到困难，请查看[社区支持资源](https://flink.apache.org/community.html)。
特别地，Apache Flink的[用户邮件列表](https://flink.apache.org/community.html#mailing-lists)始终被列为所有Apache项目中最活跃的项目之一，也是快速获得帮助的好方法。

{{< hint info >}}
如果在windows上运行docker并且您的数据生成器容器无法启动，请确保您使用的是正确的shell。
例如，**table-walkthrough_data-generator_1**容器的**docker-entrypoint.sh**需要bash。
如果不可用，它将引发错误**standard_init_linux.go:211: exec user process caused "no such file or directory"**。
一种解决方法是在**docker-entrypoint.sh**的第一行将shell切换到**sh**。
{{< /hint >}}

## 如何操作本教程

如果要遵循该教程，则需要一台计算机和以下环境：

* Java 8 or 11
* Maven 
* Docker

{{< unstable >}}
{{< hint warning >}}
**注意：** 用于此教程的Apache Flink Docker镜像仅适用于已发布的Apache Flink版本。

由于您当前正在查看最新快照版本的文档，下面的所有版本引用都将不起作用。
请通过菜单下方左侧的release picker将文档切换到最新版本。
{{< /hint >}}
{{< /unstable >}}

所需的配置文件在[flink-playgrounds](https://github.com/apache/flink-playgrounds)存储库中可用。
下载后，在IDE中打开项目`flink-playground/table-walkthrough`，然后导航到文件`SpendReport`。

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
TableEnvironment tEnv = TableEnvironment.create(settings);

tEnv.executeSql("CREATE TABLE transactions (\n" +
    "    account_id  BIGINT,\n" +
    "    amount      BIGINT,\n" +
    "    transaction_time TIMESTAMP(3),\n" +
    "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
    ") WITH (\n" +
    "    'connector' = 'kafka',\n" +
    "    'topic'     = 'transactions',\n" +
    "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
    "    'format'    = 'csv'\n" +
    ")");

tEnv.executeSql("CREATE TABLE spend_report (\n" +
    "    account_id BIGINT,\n" +
    "    log_ts     TIMESTAMP(3),\n" +
    "    amount     BIGINT\n," +
    "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
    ") WITH (\n" +
    "   'connector'  = 'jdbc',\n" +
    "   'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
    "   'table-name' = 'spend_report',\n" +
    "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
    "   'username'   = 'sql-demo',\n" +
    "   'password'   = 'demo-sql'\n" +
    ")");

Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");

```

## 分解代码

#### 执行环境

前两行用来设置您的`TableEnvironment`。
表环境是您可以为作业设置属性、指定是编写批处理还是流式应用程序以及创建源的方式。
本教程创建了一个使用流执行的标准表环境。

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

#### 注册表

接下来，表将在当前 [catalog]({{< ref "docs/dev/table/catalogs" >}})中注册，您可以使用这些表连接到外部系统以读写批处理和流数据。
表源提供对存储在外部系统 (如数据库、键值存储、消息队列或文件系统) 中的数据的访问。
表接收器将表发送到外部存储系统。
根据源和接收器的类型，它们支持不同的格式，例如CSV，JSON，Avro或Parquet。

```java
tEnv.executeSql("CREATE TABLE transactions (\n" +
     "    account_id  BIGINT,\n" +
     "    amount      BIGINT,\n" +
     "    transaction_time TIMESTAMP(3),\n" +
     "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
     ") WITH (\n" +
     "    'connector' = 'kafka',\n" +
     "    'topic'     = 'transactions',\n" +
     "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
     "    'format'    = 'csv'\n" +
     ")");
```

上面代码注册了两个表：交易输入表和支出报告输出表。
交易 (transactions) 表允许我们读取信用卡交易，其中包含帐户ID(`account_id`) 、时间戳(`transaction_time`)和美元金额(`amount`)。
该表是Kafka主题的逻辑视图，该主题称为包含CSV数据的事务。

```java
tEnv.executeSql("CREATE TABLE spend_report (\n" +
    "    account_id BIGINT,\n" +
    "    log_ts     TIMESTAMP(3),\n" +
    "    amount     BIGINT\n," +
    "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
    ") WITH (\n" +
    "    'connector'  = 'jdbc',\n" +
    "    'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
    "    'table-name' = 'spend_report',\n" +
    "    'driver'     = 'com.mysql.jdbc.Driver',\n" +
    "    'username'   = 'sql-demo',\n" +
    "    'password'   = 'demo-sql'\n" +
    ")");
```

第二个表`spend_report`存储聚合的最终结果。
它的底层存储是MySql数据库中的表。

#### 查询

配置了环境并注册了表，您就可以构建第一个应用程序了。
从`TableEnvironment`中，您可以从输入表中读取其行，然后使用`executeInsert`将这些结果写入输出表中。
`report` function是您将实现业务逻辑的地方。
目前尚未实现。

```java
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## 测试

该项目包含辅助测试类`SpendReportTest`，用于验证报告的逻辑。
它以批处理模式创建表环境。

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings); 
```

Flink的独特属性之一是它提供跨批处理和流的一致语义。
这意味着您可以在静态数据集上以批处理模式开发和测试应用程序，并将其作为流应用程序部署到生产中。

## 一次尝试

现在有了作业设置的框架，您就可以添加一些业务逻辑了。
我们的目标是生成一个报告，显示一天中每个小时每个帐户的总支出。
这意味着时间戳列需要从毫秒粒度向下舍入到小时粒度。

Flink支持在纯[SQL]({{< ref "docs/dev/table/sql/overview" >}})或使用[Table API]({{< ref "docs/dev/table/tableApi" >}})中开发关系应用程序。
表API是一个受SQL启发的流畅的DSL，可以用Python、Java或Scala编写，并支持强大的IDE集成。
就像SQL查询一样，表程序可以根据您的键选择所需的字段和分组。
这些功能，与像`floor`和`sum`一样的[内置函数]({{< ref "docs/dev/table/functions/systemFunctions" >}})，可以写这个报告。

```java
public static Table report(Table transactions) {
    return transactions.select(
            $("account_id"),
            $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
            $("amount"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts"),
            $("amount").sum().as("amount"));
}
```

## 用户自定义函数

Flink包含有限数量的内置函数，有时需要用[用户定义函数]({{< ref "docs/dev/table/functions/udfs" >}})进行扩展。
如果`floor`没有预定义，你可以自己实现。

```java
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
        @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
```

之后可以将其快速集成到您的应用程序中。

```java
public static Table report(Table transactions) {
    return transactions.select(
            $("account_id"),
            call(MyFloor.class, $("transaction_time")).as("log_ts"),
            $("amount"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts"),
            $("amount").sum().as("amount"));
}
```

This query consumes all records from the `transactions` table, calculates the report, and outputs the results in an efficient, scalable manner.
Running the test with this implementation will pass. 
此查询会消耗事务表中的所有记录，计算报表并以高效，可扩展的方式输出结果。
使用此实现运行测试将通过。

## 添加窗口

Grouping data based on time is a typical operation in data processing, especially when working with infinite streams.
A grouping based on time is called a [window]({{< ref "docs/dev/datastream/operators/windows" >}}) and Flink offers flexible windowing semantics.
The most basic type of window is called a `Tumble` window, which has a fixed size and whose buckets do not overlap.
根据时间对数据进行分组是数据处理中的典型操作，尤其是在处理无限流时。
基于时间的分组称为[窗口]({{< ref "docs/dev/datastream/operators/windows" >}})，Flink提供了灵活的窗口语义。
最基本的窗口类型称为滚动窗口，它具有固定的大小，并且桶与桶之间不重叠。

```java
public static Table report(Table transactions) {
    return transactions
        .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts").start().as("log_ts"),
            $("amount").sum().as("amount"));
}
```

这将您的应用程序定义为使用基于时间戳列的一小时的滚动窗口。
时间戳为`2019-06-01 01:23:47`的行将被放入`2019-06-01 01:00:00`窗口中。


基于时间的聚合是唯一的，因为与其他属性相反，时间通常在连续流应用程序中向前移动。
与`floor`和您的UDF不同，窗口函数是[内建函数](https://en.wikipedia.org/wiki/Intrinsic_function)，它允许运行时应用额外的优化。
在批处理上下文中，窗口提供了一种方便的API，用于按时间戳属性对记录进行分组。

使用此实现运行测试也将通过。

## 再来一次，使用流处理！

以上就是一个功能齐全，有状态的分布式流应用程序！
查询会持续消耗Kafka的事务流，计算每小时的消费，并在结果准备就绪后立即发出结果。
由于输入是无界的，因此查询一直运行，直到手动停止为止。
而且由于作业使用基于时间窗口的聚合，因此Flink可以在框架知道没有更多记录将到达特定窗口时执行特定的优化，例如状态清理。

Table playground已完全docker化，并且可以作为流应用程序在本地运行。
该环境包含Kafka主题、连续数据生成器、MySql和Grafana。

从`table-walkthrough`文件夹中启动docker-compose脚本。

```bash
$ docker-compose build
$ docker-compose up -d
```

您可以通过[Flink控制台](http://localhost:8082/)查看正在运行的作业的信息。

{{< img src="/fig/spend-report-console.png" height="400px" width="800px" alt="Flink Console">}}

从MySQL内部查询结果。

```bash
$ docker-compose exec mysql mysql -Dsql-demo -usql-demo -pdemo-sql

mysql> use sql-demo;
Database changed

mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|      110 |
+----------+
```

最后，去[Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s)中查看完全可视化的结果!

{{< img src="/fig/spend-report-grafana.png" alt="Grafana" >}}
