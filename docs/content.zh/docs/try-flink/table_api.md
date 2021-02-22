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

Apache Flink 提供了 Table API 作为统一的关系API，用于批处理和流处理，即：对无边界的实时流或有约束的批处理数据集以相同的语义执行查询，并产生相同的结果。Flink 中的 Table API 通常用于简化数据分析，数据管道和ETL应用程序的定义。

## 您要搭建一个什么系统

在本教程中，您将学习如何构建实时仪表板以按帐户跟踪财务交易。流程将从Kafka读取数据，并将结果写入通过 Grafana 可视化的 MySQL。

## 准备条件

这个代码练习假定您对 Java 或 Scala 有一定的了解，当然，如果您之前使用的是其他开发语言，您也应该能够跟随本教程进行学习。同时假定您熟悉基本的关系概念，例如 SELECT 和 GROUP BY 语法。

## 困难求助

如果遇到困难，可以参考[社区支持资源](https://flink.apache.org/community.html)。 当然也可以在邮件列表提问，Flink 的[用户邮件列表](https://flink.apache.org/community.html#mailing-lists)一直被评为所有Apache项目中最活跃的一个，这也是快速获得帮助的好方法。

{{< hint info >}}
如果在Windows上运行docker并且您的数据生成器容器无法启动，那么请确保您使用的 shell 正确。例如，**table-walkthrough_data-generator_1** 容器的 **docker-entrypoint.sh** 需要使用bash shell。如果不可用，它将导致**standard_init_linux.go:211: exec user process caused "no such file or directory"** 错误。一种解决方法是将 **docker-entrypoint.sh** 文件的的第一行切换为 **sh** shell。
{{< /hint >}}

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 8 or 11
* Maven 
* Docker

{{< unstable >}}
{{< hint warning >}}
**Attention:** The Apache Flink Docker images used for this playground are only available for released versions of Apache Flink.

Since you are currently looking at the latest SNAPSHOT
version of the documentation, all version references below will not work.
Please switch the documentation to the latest released version via the release picker which you find on the left side below the menu.
{{< /hint >}}
{{< /unstable >}}

The required configuration files are available in the [flink-playgrounds](https://github.com/apache/flink-playgrounds) repository.
Once downloaded, open the project `flink-playground/table-walkthrough` in your IDE and navigate to the file `SpendReport`. 

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

## Breaking Down The Code

#### The Execution Environment

The first two lines set up your `TableEnvironment`.
The table environment is how you can set properties for your Job, specify whether you are writing a batch or a streaming application, and create your sources.
This walkthrough creates a standard table environment that uses the streaming execution.

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

#### Registering Tables

Next, tables are registered in the current [catalog]({{< ref "docs/dev/table/catalogs" >}}) that you can use to connect to external systems for reading and writing both batch and streaming data.
A table source provides access to data stored in external systems, such as a database, a key-value store, a message queue, or a file system.
A table sink emits a table to an external storage system.
Depending on the type of source and sink, they support different formats such as CSV, JSON, Avro, or Parquet.

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

Two tables are registered; a transaction input table, and a spend report output table.
The transactions (`transactions`) table lets us read credit card transactions, which contain account ID's (`account_id`), timestamps (`transaction_time`), and US$ amounts (`amount`).
The table is a logical view over a Kafka topic called `transactions` containing CSV data.

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

The second table, `spend_report`, stores the final results of the aggregation.
Its underlying storage is a table in a MySql database.

#### The Query

With the environment configured and tables registered, you are ready to build your first application.
From the `TableEnvironment` you can read `from` an input table to read its rows and then write those results into an output table using `executeInsert`.
The `report` function is where you will implement your business logic.
It is currently unimplemented.

```java
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## Testing 

The project contains a secondary testing class `SpendReportTest` that validates the logic of the report.
It creates a table environment in batch mode. 

```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings); 
```

One of Flink's unique properties is that it provides consistent semantics across batch and streaming.
This means you can develop and test applications in batch mode on static datasets, and deploy to production as streaming applications.

## Attempt One

Now with the skeleton of a Job set-up, you are ready to add some business logic.
The goal is to build a report that shows the total spend for each account across each hour of the day.
This means the timestamp column needs be be rounded down from millisecond to hour granularity. 

Flink supports developing relational applications in pure [SQL]({{< ref "docs/dev/table/sql/overview" >}}) or using the [Table API]({{< ref "docs/dev/table/tableApi" >}}).
The Table API is a fluent DSL inspired by SQL, that can be written in Python, Java, or Scala and supports strong IDE integration.
Just like a SQL query, Table programs can select the required fields and group by your keys.
These features, allong with [built-in functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}) like `floor` and `sum`, you can write this report.

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

## User Defined Functions

Flink contains a limited number of built-in functions, and sometimes you need to extend it with a [user-defined function]({{< ref "docs/dev/table/functions/udfs" >}}).
If `floor` wasn't predefined, you could implement it yourself. 

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

And then quickly integrate it in your application.

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

## Adding Windows

Grouping data based on time is a typical operation in data processing, especially when working with infinite streams.
A grouping based on time is called a [window]({{< ref "docs/dev/datastream/operators/windows" >}}) and Flink offers flexible windowing semantics.
The most basic type of window is called a `Tumble` window, which has a fixed size and whose buckets do not overlap.

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

This defines your application as using one hour tumbling windows based on the timestamp column.
So a row with timestamp `2019-06-01 01:23:47` is put in the `2019-06-01 01:00:00` window.


Aggregations based on time are unique because time, as opposed to other attributes, generally moves forward in a continuous streaming application.
Unlike `floor` and your UDF, window functions are [intrinsics](https://en.wikipedia.org/wiki/Intrinsic_function), which allows the runtime to apply additional optimizations.
In a batch context, windows offer a convenient API for grouping records by a timestamp attribute.

Running the test with this implementation will also pass. 

## Once More, With Streaming!

And that's it, a fully functional, stateful, distributed streaming application!
The query continuously consumes the stream of transactions from Kafka, computes the hourly spendings, and emits results as soon as they are ready.
Since the input is unbounded, the query keeps running until it is manually stopped.
And because the Job uses time window-based aggregations, Flink can perform specific optimizations such as state clean up when the framework knows that no more records will arrive for a particular window.

The table playground is fully dockerized and runnable locally as streaming application.
The environment contains a Kafka topic, a continuous data generator, MySql, and Grafana. 

From within the `table-walkthrough` folder start the docker-compose script.

```bash
$ docker-compose build
$ docker-compose up -d
```

You can see information on the running job via the [Flink console](http://localhost:8082/).

![Flink Console]({% link /fig/spend-report-console.png %}){:height="400px" width="800px"}

Explore the results from inside MySQL.

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

Finally, go to [Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s) to see the fully visualized result!

{{< img src="/fig/spend-report-grafana.png" alt="Grafana" >}}
