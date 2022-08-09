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

Apache Flink 提供 Table API 作为批流统一的关系型 API，例如，相同语法的查询在无界的实时流数据或者有界的批数据集上执行会得到一致的结果。  
Table API 在 Flink 中常被用于简化数据分析、数据流水线以及 ETL 应用的定义。

## 你接下来要搭建的是什么系统？

在本教程中，你会学习到如何创建一个按账户追踪金融交易的实时看板。  
整条流水线为读 Kafka 中的数据并将结果写入到 MySQL 通过 Grafana 展示。

## 准备条件

本次代码练习假定你对 Java 或者 Scala 有一定的了解，当然如果你之前使用的是其他编程语言，也应该可以继续学习。  
我们也假设你对关系型概念例如 `SELECT` 以及 `GROUP BY` 语句有一定的了解。

## 困难求助

如果遇到问题，可以参考 [社区支持资源](https://flink.apache.org/community.html)。
Flink 的 [用户邮件列表](https://flink.apache.org/community.html#mailing-lists) 是 Apahe 项目中最活跃的一个，这也是快速获得帮助的一个好方法。

{{< hint info >}}
如果你是在 Windows 上运行的 docker，并且生成数据的容器启动失败了，可以检查下是否使用了正确的脚本。
例如 **docker-entrypoint.sh** 是容器 **table-walkthrough_data-generator_1** 所需的 bash 脚本。
如果不可用，会报 **standard_init_linux.go:211: exec user process caused "no such file or directory"** 的错误。
一种解决办法是在 **docker-entrypoint.sh** 的第一行将脚本执行器切换到 **sh**
{{< /hint >}}

## 如何跟着教程练习

在电脑上安装如下环境： 

* Java 11
* Maven 
* Docker

{{< unstable >}}
{{< hint warning >}}
**注意：** 本文中使用的 Apache Flink Docker 镜像仅适用于 Apache Flink 发行版。

 由于你目前正在浏览快照版的文档，因此下文中引用的分支可能已经不存在了，请先通过左侧菜单下方的版本选择器切换到发行版文档再查看。
{{< /hint >}}
{{< /unstable >}}

我们使用的配置文件位于 [flink-playgrounds](https://github.com/apache/flink-playgrounds) 代码仓库中，
当下载完成后，在你的 IDE 中打开 `flink-playground/table-walkthrough` 项目，并找到文件 `SpendReport`。

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
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

前两行创建 `TableEnvironment`。
在表环境中，你可以设置作业的属性，定义写的应用是批的还是流的，也可以创建 sources。
本次创建了一个使用流式执行器的标准表环境。

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

#### 注册表

接下来，在当前 [catalog]({{< ref "docs/dev/table/catalogs" >}}) 中创建表，这样就可以读写外部系统的数据了，数据可以是批的也可以是流式的。
表类型的 source 可以访问如数据库、键值类型存储、消息队列或者文件系统这些外部系统中的数据。
表类型的 sink 会按照表的形式写数据到外部存储系统。
根据 source 以及 sink 的类型，可以支持不同的存储格式，例如 CSV, JSON, Avro, 或者 Parquet。

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

这里注册了两张表：一张存交易数据的输入表，一张存消费报告的输出表。
交易表（`transactions`）存的是信用卡交易记录，记录包含账户 ID（`account_id`），交易时间（`transaction_time`），以及美元单位的金额（`amount`）。
输入表是 Kafka topic 的逻辑视图，topic 为 `transactions`，按 CSV 格式存储的数据。

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

第二张 `spend_report` 表存储聚合后的最终结果，底层存储是 MySQL 数据库。

#### 查询数据

在准备好环境配置并注册好表之后，你就可以开始开发你的第一个应用了。
在 `TableEnvironment` 中你可以 `from` 输入表来读取数据并将结果通过 `executeInsert` 写入到输出表中。
你可以在函数 `report` 中实现具体的业务逻辑，这里暂时还没有实现。

```java
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## 测试 

当前项目包含一个辅助验证报表逻辑的测试类 `SpendReportTest`，该测试的表环境使用的是 batch 模式。

```java
EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
TableEnvironment tEnv = TableEnvironment.create(settings); 
```

Flink 的一个独特特性是提供了批流统一的语义，这意味着你可以用静态数据集在 batch 模式下来开发和测试应用，而到生产环境式部署为流式应用。

## 尝试下

现在，有了搭建好的环境，你就可以开始实现一些业务逻辑了。现在的目标是创建一个报表，报表按照账户显示一天中每个小时的总支出。因此毫秒粒度的时间戳字段需要向下舍入为小时。 

Flink 支持纯 [SQL]({{< ref "docs/dev/table/sql/overview" >}}) 或者 [Table API]({{< ref "docs/dev/table/tableApi" >}}) 开发关系型数据应用。

Table API 是受 SQL 启发设计出的一套链式 DSL，可以用 Python、Java 或者 Scala 开发，在 IDE 中也集成的很好。同时也如 SQL 查询一样，Table 应用可以只查询指定的字段，或者按指定列做分组。
通过类似 `floor` 以及 `sum` 这样的 [系统函数]({{< ref "docs/dev/table/functions/systemFunctions" >}})，你就可以开发这个报表了。

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

## 用户定义函数

Flink 内置的函数是有限的，有时是需要通过 [用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})来拓展这些函数。
假如没有预设好的 `floor` 函数，那么你就可以自己实现一个。

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

然后就可以马上在你的应用中使用了。

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

这条查询会从表 `transactions` 消费所有的记录，计算出报表内容，并将结果以高效、可拓展的方式输出。
测试用例使用此实现可以通过。

## 添加窗口函数

在数据处理中，按照时间做分组是一个典型的操作，尤其是在处理无限流时。
用于按时间分组的函数叫 [window]({{< ref "docs/dev/datastream/operators/windows" >}})，Flink 提供了灵活的窗口函数语法。
最常见的窗口是 `Tumble` ，此窗口固定窗口区间并且每个区间都不重叠。

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

上面为应用定义了一个基于时间戳字段的一小时区间大小的滚动窗口，因此时间戳为 `2019-06-01 01:23:47` 的行会进入窗口 `2019-06-01 01:00:00`。


基于时间的聚合是不重复的，因为时间不同于其他属性，在一个持续性的流式应用中，时间通常是向前移动的。

不同于 `floor` 以及自己写的 UDF，窗口函数是 [内部的][intrinsics](https://en.wikipedia.org/wiki/Intrinsic_function)，这意味着会使用更多的运行时优化。
在一个批上下文中，窗口函数是一个按时间属性分组数据的便利 API。

测试用例使用此实现也是可以通过的。

## 再用流式处理一次！

这次的应用是一个功能齐全、有状态的分布式流式应用！
查询语句从 Kafka 中持续消费流式的交易数据，并计算每小时的消费，只要计算好就提交结果。
由于输入是无边界的，查询在手动停止前会一直运行。
同时，由于作业使用了窗口函数，Flink 会执行一些特定的优化，例如当框架感知到特定的窗口不会再来数据时就释放状态数据。

这次演示是完全容器化的，并可以在本地运行的流式应用。
环境中包含了 Kafka 的 topic、持续数据生成器、MySQL 以及 Grafana。

在 `table-walkthrough` 目录下启动 docker-compose 脚本。

```bash
$ docker-compose build
$ docker-compose up -d
```

你可以通过 [Flink console](http://localhost:8082/) 来查看运行中任务的信息。

{{< img src="/fig/spend-report-console.png" height="400px" width="800px" alt="Flink Console">}}

在 MySQL 中查看结果数据。

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

最后，完整的可视化结果可以访问 [Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s)！

{{< img src="/fig/spend-report-grafana.png" alt="Grafana" >}}
