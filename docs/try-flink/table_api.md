---
title: "Real Time Reporting with the Table API"
nav-title: 'Real Time Reporting with the Table API'
nav-parent_id: try-flink
nav-pos: 3
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

Apache Flink offers a Table API as a unified, relational API for batch and stream processing, i.e., queries are executed with the same semantics on unbounded, real-time streams or bounded, batch data sets and produce the same results.
The Table API in Flink is commonly used to ease the definition of data analytics, data pipelining, and ETL applications.

* This will be replaced by the TOC
{:toc}

## What Will You Be Building? 

In this tutorial, you will learn how to build a real-time dashboard to track financial transactions by account.
The pipeline will read data from Kafka and write the results to MySQL visualized via Grafana.

## Prerequisites

This walkthrough assumes that you have some familiarity with Java or Scala, but you should be able to follow along even if you come from a different programming language.
It also assumes that you are familiar with basic relational concepts such as `SELECT` and `GROUP BY` clauses.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

<div class="alert alert-info">
If running docker on windows and your data generator container is failing to start, then please ensure that you're using the right shell.
For example <b>docker-entrypoint.sh</b> for <b>table-walkthrough_data-generator_1</b> container requires bash.
If unavailable, it will throw an error <b>standard_init_linux.go:211: exec user process caused "no such file or directory"</b>.
A workaround is to switch the shell to <b>sh</b> on the first line of <b>docker-entrypoint.sh</b>.
</div>

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 8 or 11
* Maven 
* Docker

{% if site.version contains "SNAPSHOT" %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
  <b>
  NOTE: The Apache Flink Docker images used for this playground are only available for
  released versions of Apache Flink.
  </b><br>
  Since you are currently looking at the latest SNAPSHOT
  version of the documentation, all version references below will not work.
  Please switch the documentation to the latest released version via the release picker which you
  find on the left side below the menu.
</p>
{% endif %}

The required configuration files are available in the [flink-playgrounds](https://github.com/apache/flink-playgrounds) repository.
Once downloaded, open the project `flink-playground/table-walkthrough` in your IDE and navigate to the file `SpendReport`. 

{% highlight java %}
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

{% endhighlight %}

## Breaking Down The Code

#### The Execution Environment

The first two lines set up your `TableEnvironment`.
The table environment is how you can set properties for your Job, specify whether you are writing a batch or a streaming application, and create your sources.
This walkthrough creates a standard table environment that uses the streaming execution.

{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
{% endhighlight %}

#### Registering Tables

Next, tables are registered in the current [catalog]({% link dev/table/catalogs.md %}) that you can use to connect to external systems for reading and writing both batch and streaming data.
A table source provides access to data stored in external systems, such as a database, a key-value store, a message queue, or a file system.
A table sink emits a table to an external storage system.
Depending on the type of source and sink, they support different formats such as CSV, JSON, Avro, or Parquet.

{% highlight java %}
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
{% endhighlight %}

Two tables are registered; a transaction input table, and a spend report output table.
The transactions (`transactions`) table lets us read credit card transactions, which contain account ID's (`account_id`), timestamps (`transaction_time`), and US$ amounts (`amount`).
The table is a logical view over a Kafka topic called `transactions` containing CSV data.

{% highlight java %}
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
{% endhighlight %}

The second table, `spend_report`, stores the final results of the aggregation.
Its underlying storage is a table in a MySql database.

#### The Query

With the environment configured and tables registered, you are ready to build your first application.
From the `TableEnvironment` you can read `from` an input table to read its rows and then write those results into an output table using `executeInsert`.
The `report` function is where you will implement your business logic.
It is currently unimplemented.

{% highlight java %}
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
{% endhighlight %}

## Testing 

The project contains a secondary testing class `SpendReportTest` that validates the logic of the report.
It creates a table environment in batch mode. 

{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings); 
{% endhighlight %}

One of Flink's unique properties is that it provides consistent semantics across batch and streaming.
This means you can develop and test applications in batch mode on static datasets, and deploy to production as streaming applications.

## Attempt One

Now with the skeleton of a Job set-up, you are ready to add some business logic.
The goal is to build a report that shows the total spend for each account across each hour of the day.
This means the timestamp column needs be be rounded down from millisecond to hour granularity. 

Flink supports developing relational applications in pure [SQL]({% link dev/table/sql/index.md %}) or using the [Table API]({% link dev/table/tableApi.md %}).
The Table API is a fluent DSL inspired by SQL, that can be written in Python, Java, or Scala and supports strong IDE integration.
Just like a SQL query, Table programs can select the required fields and group by your keys.
These features, allong with [built-in functions]({% link dev/table/functions/systemFunctions.md %}) like `floor` and `sum`, you can write this report.

{% highlight java %}
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
{% endhighlight %}

## User Defined Functions

Flink contains a limited number of built-in functions, and sometimes you need to extend it with a [user-defined function]({% link dev/table/functions/udfs.md %}).
If `floor` wasn't predefined, you could implement it yourself. 

{% highlight java %}
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
{% endhighlight %}

And then quickly integrate it in your application.

{% highlight java %}
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
{% endhighlight %}

This query consumes all records from the `transactions` table, calculates the report, and outputs the results in an efficient, scalable manner.
Running the test with this implementation will pass. 

## Adding Windows

Grouping data based on time is a typical operation in data processing, especially when working with infinite streams.
A grouping based on time is called a [window]({% link dev/stream/operators/windows.md %}) and Flink offers flexible windowing semantics.
The most basic type of window is called a `Tumble` window, which has a fixed size and whose buckets do not overlap.

{% highlight java %}
public static Table report(Table transactions) {
    return transactions
        .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts").start().as("log_ts"),
            $("amount").sum().as("amount"));
}
{% endhighlight %}

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

{% highlight bash %}
$ docker-compose build
$ docker-compose up -d
{% endhighlight %}

You can see information on the running job via the [Flink console](http://localhost:8082/).

![Flink Console]({% link /fig/spend-report-console.png %}){:height="400px" width="800px"}

Explore the results from inside MySQL.

{% highlight bash %}
$ docker-compose exec mysql mysql -Dsql-demo -usql-demo -pdemo-sql

mysql> use sql-demo;
Database changed

mysql> select count(*) from spend_report;
+----------+
| count(*) |
+----------+
|      110 |
+----------+
{% endhighlight %}

Finally, go to [Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s) to see the fully visualized result!

![Grafana]({% link /fig/spend-report-grafana.png %}){:height="400px" width="800px"}
