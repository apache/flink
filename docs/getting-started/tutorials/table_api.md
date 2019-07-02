---
title: "Table API"
nav-id: tableapitutorials
nav-title: 'Table API'
nav-parent_id: apitutorials
nav-pos: 1
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

Apache Flink offers a Table API as a unified, relational API for batch and stream processing, i.e., queries are executed with the same semantics on unbounded, real-time streams or bounded, recorded streams and produce the same results.
The Table API in Flink is commonly used to ease the definition of data analytics, data pipelining, and ETL applications.

* This will be replaced by the TOC
{:toc}

## What Are We Building? 

In this tutorial, we'll show how to build a continuous ETL pipeline for tracking financial transactions by account over time.
We will start by building our report as a nightly batch job, and then migrate to a streaming pipeline to see how batch is just a special case of streaming. 

## Prerequisites

We'll assume that you have some familiarity with Java or Scala, but you should be able to follow along even if you're coming from a different programming language.
We'll also assume that you're familiar with basic relational concepts such as `SELECT` and `GROUP BY` clauses. 

If you want to follow along, you will require a computer with: 

* Java 8 
* Maven 

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is consistently ranked as one of the most active of any Apache project and a great way to get help quickly. 

## How To Follow Along

If you would like to follow along this walkthrough provides a Flink Maven Archetype to create a skeleton project with all the necessary dependencies quickly.

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
    <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the commandline. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
</p>
{% endunless %}

You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters,
Maven will create a project with all the dependencies to complete this tutorial.
After importing the project into your editor, you will see a file following code. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

tEnv.registerTableSource("transactions", new TransactionTableSource());
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
val env  = ExecutionEnvironment.getExecutionEnvironment()
val tEnv = BatchTableEnvironment.create(env)

tEnv.registerTableSource("transactions", new TransactionTableSource())
tEnv.registerTableSink("spend_report", new SpendReportTableSink())

val truncateDateToHour = new TruncateDateToHour

tEnv
	.scan("transactions")
	.insertInto("spend_report")

env.execute("Spend Report")
{% endhighlight %}
</div>
</div>

Let's break down this code by component. 

## Breaking Down The Code

#### The Execution Environment

The first two lines set up our `ExecutionEnvironment`.
The execution environment is how we set properties for our deployments, specify whether we are writing a batch or streaming application, and create our sources.
Here we have chosen to use the batch environment since we are building a periodic batch report.
We then wrap it in a `BatchTableEnvironment` to have full access to the Table API.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env  = ExecutionEnvironment.getExecutionEnvironment();
val tEnv = BatchTableEnvironment.create(env);
{% endhighlight %}
</div>
</div>


#### Registering Tables

Next, we register tables that we can use to connect to external systems for reading and writing both batch and streaming data.
A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system).
A table sink emits a table to an external storage system.
Depending on the type of source and sink, they support different formats such as CSV, JSON, Avro, or Parquet.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tEnv.registerTableSource("transactions", new TransactionTableSource());
tEnv.registerTableSink("spend_report", new SpendReportTableSink());
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tEnv.registerTableSource("transactions", new TransactionTableSource())
tEnv.registerTableSink("spend_report", new SpendReportTableSink())
{% endhighlight %}
</div>
</div>

We register two tables, a transaction input table, and a spend report output table.
The transactions (`transactions`) table lets us read credit card transactions, which contain account ID's (`accountId`), timestamps (`timestamp`), and US$ amounts (`amount`). 
The spend report (`spend_report`) table writes the output of a job to standard output so we can easily see our results. 
Both tables support running batch and streaming applications.

#### Registering A UDF

Along with tables, we include a user-defined function for working with timestamps. Our function takes a timestamp and rounds it down to the nearest hour. 

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

With our environment configured and tables registered, we are ready to build our first application.
From the `TableEnvironment` we can `scan` an input table to read its rows and then write those results into an output table using `insertInto`.

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

Initially, the job reads all transactions and writes them to standard output.

#### Execute

Flink applications are built lazily and shipped to the cluster for execution only once fully formed. 
We call `ExecutionEnvironment#execute` to begin the execution of our job. 

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

Now that we have the skeleton of a job let's add some business logic.
We want a report that shows the total spend for each account across each hour of the day.
Just like a SQL query, we can select the required fields and group by our keys. 
Because the timestamp field has millisecond granularity, we will use our UDF to round it down to the nearest hour.
Finally, we will select all the fields, summing the total spend per account-hour pair.

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

## Adding Windows

While this works, we can do better.
The `timestamp` column represents the [event time]({{ site.baseurl }}/dev/event_time.html) of each row, where event time is the logical time when an event took place in the real world.
Flink understands the concept of event time, which we can leverage to clean up our code. 

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

This defines our application as using one hour tumbling windows based on the timestamp column.
So a row with timestamp `2019-06-01 01:23:47` is put in the `2019-06-01 01:00:00` window.

Aggregations based on time are unique because time, as opposed to other attributes, generally moves forward in a continuous streaming application. Using time window-based aggregations enables Flink to perform specific optimizations such as state clean up when the framework knows that no more records will arrive for a particular window.

## Once More, With Streaming!

With our business logic implemented, it is time to go from batch to streaming.
Because the Table API supports running against both batch and streaming runners with the same semantics, migrating is as simple as changing the execution context.

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
val env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val tEnv = StreamTableEnvironment.create(env)
{% endhighlight %}
</div>
</div>


And that's it, a fully functional, stateful, distributed streaming application!

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

        tEnv.registerTableSource("transactions", new TransactionTableSource());
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
package spendreport;

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._
import org.apache.flink.walkthrough.common.table._

object SpendReport {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        val tEnv = StreamTableEnvironment.create(env);

        tEnv.registerTableSource("transactions", new TransactionTableSource())
        tEnv.registerTableSink("spend_report", new SpendReportTableSink())

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

