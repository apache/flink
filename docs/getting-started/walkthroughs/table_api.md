---
title: "Table API"
nav-id: tableapiwalkthrough
nav-title: 'Table API'
nav-parent_id: walkthroughs
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

Apache Flink offers a Table API as a unified, relational API for batch and stream processing, i.e., queries are executed with the same semantics on unbounded, real-time streams or bounded, batch data sets and produce the same results.
The Table API in Flink is commonly used to ease the definition of data analytics, data pipelining, and ETL applications.

* This will be replaced by the TOC
{:toc}

## What Will You Be Building? 

In this tutorial, you will learn how to build a continuous ETL pipeline for tracking financial transactions by account over time.
You will start by building your report as a nightly batch job, and then migrate to a streaming pipeline.

## Prerequisites

This walkthrough assumes that you have some familiarity with Java or Scala, but you should be able to follow along even if you are coming from a different programming language.
It also assumes that you are familiar with basic relational concepts such as `SELECT` and `GROUP BY` clauses. 

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is consistently ranked as one of the most active of any Apache project and a great way to get help quickly. 

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 8 
* Maven 

A provided Flink Maven Archetype will create a skeleton project with all the necessary dependencies quickly:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-table-java \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=spend-report \
    -DartifactId=spend-report \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-table-scala \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=spend-report \
    -DartifactId=spend-report \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
{% endhighlight %}
</div>
</div>

{% unless site.is_stable %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the commandline. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven's official document</a>
</p>
{% endunless %}

You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters,
Maven will create a project with all the dependencies to complete this tutorial.
After importing the project into your editor, you will see a file with the following code which you can run directly inside your IDE.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
tEnv.registerTableSink("spend_report", new SpendReportTableSink());
tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour());

tEnv
    .scan("transactions")
    .insertInto("spend_report");

env.execute("Spend Report");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env  = ExecutionEnvironment.getExecutionEnvironment
val tEnv = BatchTableEnvironment.create(env)

tEnv.registerTableSource("transactions", new BoundedTransactionTableSource)
tEnv.registerTableSink("spend_report", new SpendReportTableSink)

val truncateDateToHour = new TruncateDateToHour

tEnv
    .scan("transactions")
    .insertInto("spend_report")

env.execute("Spend Report")
{% endhighlight %}
</div>
</div>

## Breaking Down The Code

#### The Execution Environment

The first two lines set up your `ExecutionEnvironment`.
The execution environment is how you can set properties for your Job, specify whether you are writing a batch or a streaming application, and create your sources.
This walkthrough begins with the batch environment since you are building a periodic batch report.
It is then wrapped in a `BatchTableEnvironment` to have full access to the Table API.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env  = ExecutionEnvironment.getExecutionEnvironment
val tEnv = BatchTableEnvironment.create(env)
{% endhighlight %}
</div>
</div>


#### Registering Tables

Next, tables are registered in the execution environment that you can use to connect to external systems for reading and writing both batch and streaming data.
A table source provides access to data stored in external systems; such as a database, a key-value store, a message queue, or a file system.
A table sink emits a table to an external storage system.
Depending on the type of source and sink, they support different formats such as CSV, JSON, Avro, or Parquet.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv.registerTableSource("transactions", new BoundedTransactionTableSource());
tEnv.registerTableSink("spend_report", new SpendReportTableSink());
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv.registerTableSource("transactions", new BoundedTransactionTableSource)
tEnv.registerTableSink("spend_report", new SpendReportTableSink)
{% endhighlight %}
</div>
</div>

Two tables are registered; a transaction input table, and a spend report output table.
The transactions (`transactions`) table lets us read credit card transactions, which contain account ID's (`accountId`), timestamps (`timestamp`), and US$ amounts (`amount`).
In this tutorial, the table is backed by data generated in memory to avoid any dependencies on external systems.
In practice, the `BoundedTransactionTableSource` may be backed by a filesystem, a database, or any other static source.
The spend report (`spend_report`) table logs each row with log level **INFO**, instead of writing to persistent storage, so you can easily see your results.

#### Registering A UDF

Along with the tables, a [user-defined function]({{ site.baseurl }}/dev/table/functions/udfs.html) is registered for working with timestamps.
This function takes a timestamp and rounds it down to the nearest hour.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv.registerFunction("truncateDateToHour", new TruncateDateToHour());
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val truncateDateToHour = new TruncateDateToHour
{% endhighlight %}
</div>
</div>

#### The Query

With the environment configured and tables registered, you are ready to build your first application.
From the `TableEnvironment` you can `scan` an input table to read its rows and then write those results into an output table using `insertInto`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv
    .scan("transactions")
    .insertInto("spend_report");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv
    .scan("transactions")
    .insertInto("spend_report")
{% endhighlight %}
</div>
</div>

Initially, the Job reads all transactions and logs them out with log level **INFO**.

#### Execute

Flink applications are built lazily and shipped to the cluster for execution only once fully formed.
You call `ExecutionEnvironment#execute` to begin the execution of your Job by giving it a name.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.execute("Spend Report");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
env.execute("Spend Report")
{% endhighlight %}
</div>
</div>

## Attempt One

Now with the skeleton of a Job set-up, you are ready to add some business logic.
The goal is to build a report that shows the total spend for each account across each hour of the day.
Just like a SQL query, Flink can select the required fields and group by your keys.
Because the timestamp field has millisecond granularity, you can use the UDF to round it down to the nearest hour.
Finally, select all the fields, summing the total spend per account-hour pair with the built-in `sum` [aggregate function]({{ site.baseurl }}/dev/table/functions/systemFunctions.html#aggregate-functions).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv
    .scan("transactions")
    .select("accountId, timestamp.truncateDateToHour as timestamp, amount")
    .groupBy("accountId, timestamp")
    .select("accountId, timestamp, amount.sum as total")
    .insertInto("spend_report");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv
    .scan("transactions")
    .select('accountId, truncateDateToHour('timestamp) as 'timestamp, 'amount)
    .groupBy('accountId, 'timestamp)
    .select('accountId, 'timestamp, 'amount.sum as 'total)
    .insertInto("spend_report")
{% endhighlight %}
</div>
</div>

This query consumes all records from the `transactions` table, calculates the report, and outputs the results in an efficient, scalable manner.

{% highlight raw %}
# Query 1 output showing account id, timestamp, and amount

> 1, 2019-01-01 00:00:00.0, $567.87
> 2, 2019-01-01 00:00:00.0, $726.23
> 1, 2019-01-01 01:00:00.0, $686.87
> 2, 2019-01-01 01:00:00.0, $810.06
> 1, 2019-01-01 02:00:00.0, $859.35
> 2, 2019-01-01 02:00:00.0, $458.40
> 1, 2019-01-01 03:00:00.0, $330.85
> 2, 2019-01-01 03:00:00.0, $730.02
> 1, 2019-01-01 04:00:00.0, $585.16
> 2, 2019-01-01 04:00:00.0, $760.76
{% endhighlight %}

## Adding Windows

Grouping data based on time is a typical operation in data processing, especially when working with infinite streams.
A grouping based on time is called a [window]({{ site.baseurl }} /dev/stream/operators/windows.html) and Flink offers flexible windowing semantics.
The most basic type of window is called a `Tumble` window, which has a fixed size and whose buckets do not overlap.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv
    .scan("transactions")
    .window(Tumble.over("1.hour").on("timestamp").as("w"))
    .groupBy("accountId, w")
    .select("accountId, w.start as timestamp, amount.sum")
    .insertInto("spend_report");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv
    .scan("transactions")
    .window(Tumble over 1.hour on 'timestamp as 'w)
    .groupBy('accountId, 'w)
    .select('accountId, 'w.start as 'timestamp, 'amount.sum)
    .insertInto("spend_report")
{% endhighlight %}
</div>
</div>

This defines your application as using one hour tumbling windows based on the timestamp column.
So a row with timestamp `2019-06-01 01:23:47` is put in the `2019-06-01 01:00:00` window.

Aggregations based on time are unique because time, as opposed to other attributes, generally moves forward in a continuous streaming application.
In a batch context, windows offer a convenient API for grouping records by a timestamp attribute.

Running the updated query will produce identical results as before.

{% highlight raw %}
# Query 2 output showing account id, timestamp, and amount

> 1, 2019-01-01 00:00:00.0, $567.87
> 2, 2019-01-01 00:00:00.0, $726.23
> 1, 2019-01-01 01:00:00.0, $686.87
> 2, 2019-01-01 01:00:00.0, $810.06
> 1, 2019-01-01 02:00:00.0, $859.35
> 2, 2019-01-01 02:00:00.0, $458.40
> 1, 2019-01-01 03:00:00.0, $330.85
> 2, 2019-01-01 03:00:00.0, $730.02
> 1, 2019-01-01 04:00:00.0, $585.16
> 2, 2019-01-01 04:00:00.0, $760.76
{% endhighlight %}

## Once More, With Streaming!

Because Flink's Table API offers consistent syntax and semantics for both batch and streaming, migrating from one to the other requires just two steps.

The first step is to replace the batch `ExecutionEnvironment` with its streaming counterpart, `StreamExecutionEnvironment`, which creates a continuous streaming Job.
It includes stream-specific configurations, such as the time characteristic, which when set to [event time]({{ site.baseurl }}/dev/event_time.html) guarantees consistent results even when faced with out-of-order events or a Job failure.
This is what will be used by your `Tumble` window when grouping records.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val tEnv = StreamTableEnvironment.create(env)
{% endhighlight %}
</div>
</div>

The second step is to migrate from a bounded data source to an infinite data source.
The project comes with an `UnboundedTransactionTableSource` that continuously creates transaction events in real-time.
Similar to the `BoundedTransactionTableSource` this table is backed by data generated in memory to avoid any dependencies on external systems.
In practice, this table might read from a streaming source such as Apache Kafka, AWS Kinesis, or Pravega.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource());
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource)
{% endhighlight %}
</div>
</div>

And that's it, a fully functional, stateful, distributed streaming application!
The query continuously consumes the stream of transactions, computes the hourly spendings, and emits results as soon as they are ready.
Since the input is unbounded, the query keeps running until it is manually stopped.
And because the Job uses time window-based aggregations, Flink can perform specific optimizations such as state clean up when the framework knows that no more records will arrive for a particular window.

{% highlight raw %}
# Query 3 output showing account id, timestamp, and amount

# These rows are calculated continuously over the hour 
# and output immediately at the end of the hour
> 1, 2019-01-01 00:00:00.0, $567.87
> 2, 2019-01-01 00:00:00.0, $726.23

# Flink begins computing these rows as soon as 
# as the first record for the window arrives
> 1, 2019-01-01 01:00:00.0, $686.87
> 2, 2019-01-01 01:00:00.0, $810.06

{% endhighlight %}

## Final Application

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
package spendreport;

import org.apache.flink.walkthrough.common.table.SpendReportTableSink;
import org.apache.flink.walkthrough.common.table.TransactionTableSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class SpendReport {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource());
        tEnv.registerTableSink("spend_report", new SpendReportTableSink());

        tEnv
            .scan("transactions")
            .window(Tumble.over("1.hour").on("timestamp").as("w"))
            .groupBy("accountId, w")
            .select("accountId, w.start as timestamp, amount.sum")
            .insertInto("spend_report");

        env.execute("Spend Report");
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
package spendreport

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._
import org.apache.flink.walkthrough.common.table._

object SpendReport {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val tEnv = StreamTableEnvironment.create(env)

        tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource)
        tEnv.registerTableSink("spend_report", new SpendReportTableSink)

        tEnv
            .scan("transactions")
            .window(Tumble over 1.hour on 'timestamp as 'w)
            .groupBy('accountId, 'w)
            .select('accountId, 'w.start as 'timestamp, 'amount.sum)
            .insertInto("spend_report")

        env.execute("Spend Report")
    }
}
{% endhighlight %}
</div>
</div>

