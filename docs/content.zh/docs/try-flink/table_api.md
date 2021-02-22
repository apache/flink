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

Apache Flink 提供了 Table API 作为统一的相关 API，用于批处理和流处理，即：对无边界的实时流或有约束的批处理数据集以相同的语义执行查询，并产生相同的结果。Flink 中的 Table API 通常用于简化数据分析，数据管道和ETL应用程序的定义。

## 您要搭建一个什么系统

在本教程中，您将学习如何构建实时仪表板以按帐户跟踪财务交易。流程将从Kafka读取数据，并将结果写入通过 Grafana 可视化的 MySQL。

## 准备条件

这个代码练习假定您对 Java 或 Scala 有一定的了解，当然，如果您之前使用的是其他开发语言，您也应该能够跟随本教程进行学习。同时假定您熟悉基本的关系概念，例如 SELECT 和 GROUP BY 语法。

## 困难求助

如果遇到困难，可以参考[社区支持资源](https://flink.apache.org/community.html)。 当然也可以在邮件列表提问，Flink 的[用户邮件列表](https://flink.apache.org/community.html#mailing-lists)一直被评为所有Apache项目中最活跃的一个，这也是快速获得帮助的好方法。

{{< hint info >}}
如果在Windows上运行docker并且您的数据生成器容器无法启动，那么请确保您使用的 shell 正确。例如，**table-walkthrough_data-generator_1** 容器的 **docker-entrypoint.sh** 需要使用bash shell。如果不可用，它将导致**standard_init_linux.go:211: exec user process caused "no such file or directory"** 错误。一种解决方法是将 **docker-entrypoint.sh** 文件的的第一行切换为 **sh** shell。
{{< /hint >}}

## 如何跟着教程练习

首先，您需要在您的电脑上准备以下环境：

* Java 8 or 11
* Maven 
* Docker

{{< unstable >}}
{{< hint warning >}}
**注意：** 此练习使用的Apache Flink Docker 镜像仅适用于Apache Flink发行版本。

由于您当前正在查看文档的最新 SNAPSHOT 版本，因此以下所有版本参考均不起作用，请通过菜单下方左侧的版本选择器将文档切换到最新发行的版本。
{{< /hint >}}
{{< /unstable >}}

所需的配置文件位于 [flink-playgrounds](https://github.com/apache/flink-playgrounds) 仓库中。下载后，在您的IDE中打开 flink-playground/table-walkthrough 项目并导航至 SpendReport 文件。

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

## 代码分析

#### 执行环境

前两行设置您的TableEnvironment。表环境是您可以为Job设置属性，指定是编写批处理应用程序还是流应用程序以及创建源的方法，本练习将创建一个使用流执行的标准表环境。

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

#### 注册表

接下来，将表注册到当前的[catalog]({{< ref "docs/dev/table/catalogs" >}}) 中，您可以使用该表连接到外部系统以读取和写入批处理数据和流数据。源表提供对存储在外部系统（例如数据库、键值存储、消息队列或文件系统）中的数据的访问，目的表将表发送到外部存储系统。根据源和目的的类型，支持不同的数据类型，例如CSV、JSON、Avro 或 Parquet。

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

两张表被注册：交易输入表和支出报告输出表。
交易（`transactions`）表让我们读取信用卡交易，其中包含帐户ID（`account_id`）、时间戳（`transaction_time`）和美元金额（`amount`）。该表是有关Kafka主题（`transactions`包含CSV数据）的逻辑视图。

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

第二张表`spend_report`存储了聚合的最终结果，它的基础存储是 MySql 数据库中的表。

#### 查询

配置好环境并注册表之后，就可以构建第一个应用程序了。从 `TableEnvironment` 您可以利用 `from` 从输入表读取其行，然后利用 `executeInsert` 将结果写入输出表。该 `report` 功能是实现业务逻辑的地方，目前尚未实现。

```java
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## 测试

该项目包含一个辅助测试类 `SpendReportTest`，用于验证报告逻辑。它以批处理方式创建表环境。

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings); 
```

Flink 的独特属性之一是，它在批处理和流传输之间提供一致的语义。这意味着您可以在静态数据集上以批处理模式开发和测试应用程序，并作为流应用程序部署到生产中。

## 尝试
 
现在，有了 Job 设置的框架，您就可以添加一些业务逻辑。目的是建立一个报告，显示每个帐户在一天中每个小时的总支出。这意味着时间戳列需要从毫秒舍入到小时粒度。

Flink supports developing relational applications in pure [SQL]({{< ref "docs/dev/table/sql/overview" >}}) or using the [Table API]({{< ref "docs/dev/table/tableApi" >}}).
The Table API is a fluent DSL inspired by SQL, that can be written in Python, Java, or Scala and supports strong IDE integration.
Just like a SQL query, Table programs can select the required fields and group by your keys.
These features, allong with [built-in functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}) like `floor` and `sum`, you can write this report.
Flink支持使用纯[SQL]({{< ref "docs/dev/table/sql/overview" >}})或使用[Table API]({{< ref "docs/dev/table/tableApi" >}})。Table API 是受 SQL 启发的流畅 DSL，可以用 Python、Java或 Scala 编写，并支持强大的 IDE 集成。就像 SQL 查询一样，Table 程序可以选择必填字段并按键进行分组。这些特点，结合[内置函数] ({{< ref "docs/dev/table/functions/systemFunctions" >}}) ，如floor和sum，可以编写此报告。

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

Flink 包含有限的内置函数，有时您需要使用[用户定义的函数]({{< ref "docs/dev/table/functions/udfs" >}})对其进行扩展。比如 `floor` 函数未预定义，则可以自己实现。

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

然后将其快速集成到您的应用程序中。

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

该查询使用 `transactions` 表中的所有记录，计算报告，并以有效、可扩展的方式输出结果。使用此实现运行测试用例将通过。

## 新增窗口函数

基于时间对数据进行分组是数据处理中的典型操作，尤其是在处理无限流时。基于时间的分组称为[窗口]({{< ref "docs/dev/datastream/operators/windows" >}}) ，Flink提供了灵活的窗口语义。窗口的最基本类型称为 `Tumble` 窗口，它具有固定的大小并且其存储桶不重叠。

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

这将您的应用程序定义为使用基于timestamp列的一小时滚动窗口。因此，带有时间戳的行 `2019-06-01 01:23:47` 将被放置在 `2019-06-01 01:00:00` 窗口中。

基于时间的聚合是唯一的，因为与其它属性相反，时间通常在连续流应用程序中向前移动。与用户自定义函数 `floor` 不同，窗口函数是[内部函数](https://en.wikipedia.org/wiki/Intrinsic_function)，它允许运行时应用额外的优化。在批处理环境中，窗口函数提供了一种用于按 timestamp 属性对记录进行分组方便的API。

Running the test with this implementation will also pass. 

## Once More, With Streaming!

就这样，一个功能齐全、有状态、分布式的流应用程序构建完成！查询持续消耗来自Kafka的交易流数据，计算每小时支出，并在准备好后立即输出结果。由于输入是无界的，因此查询将一直运行，直到手动将其停止为止。并且由于作业使用基于时间窗口的聚合，因此 Flink 可以执行特定的优化，例如当框架知道不再有特定窗口的记录到达时进行状态清除。

表运行环境已完全实现容器化，可作为流应用程序在本地运行。该环境包含一个Kafka主题、一个连续数据生成器、MySql和Grafana。

从 `table-walkthrough` 文件夹中启动 docker-compose 脚本。

```bash
$ docker-compose build
$ docker-compose up -d
```

您可以通过[Flink 控制台](http://localhost:8082/)查看有关正在运行的作业的信息。

![Flink 控制台]({% link /fig/spend-report-console.png %}){:height="400px" width="800px"}

从 MySQL 内部查询结果。

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

最后，跳转到[Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s) 以查看完全可视化的结果！

{{< img src="/fig/spend-report-grafana.png" alt="Grafana" >}}
