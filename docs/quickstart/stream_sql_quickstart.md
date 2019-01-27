---
title: "Stream SQL Examples"
nav-title: Stream SQL Examples
nav-parent_id: examples
nav-pos: 24
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

## Submit SQL Query via SQL Client

The following example programs showcase different applications of Flink SQL
from simple word counting to pv-uv statistics via SQL Client and running on local standalone cluster.

Start local cluster first:

{% highlight bash %}
$ ./bin/start-cluster.sh
{% endhighlight %}

Check the web at http://localhost:8081 and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

<b>Note</b>: Before running examples below, you need to make sure the local cluster is up and running.

### Word Count
Prepare the input data:

{% highlight bash %}
$ cat /tmp/input.csv
hello
flink
hello
sql
hello
world
{% endhighlight %}

Then start SQL Client shell:

{% highlight bash %}
$ ./bin/sql-client.sh embedded
{% endhighlight %}

You can see the welcome message for flink sql client.

Paste the following sql ddl text into the shell. (For more information about sql ddl refer to [SQL]({{ site.baseurl }}/dev/table/sql.html) and [Supported DDL]({{ site.baseurl }}/dev/table/supported_ddl.html))

{% highlight bash %}
create table csv_source (
  a varchar
) with (
  type = 'csv',
  path = 'file:///tmp/input.csv'
);
{% endhighlight %}

Press 'Enter', then paste the following sql ddl text.
{% highlight bash %}
create table csv_sink (
  a varchar,
  c bigint
) with (
  type = 'csv',
  updatemode = 'upsert',
  path = 'file:///tmp/output.csv'
);
{% endhighlight %}

Press 'Enter' and paste the following sql dml text.
{% highlight bash %}
insert into csv_sink
select
  a,
  count(*)
from csv_source
group by a;
{% endhighlight %}

After press 'Enter' the sql will be submitted to the standalone cluster. The log will print on the shell.

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-run.png" alt="SQLClient Example: WordCount run"/></a>

Open [http://localhost:8081](http://localhost:8081) and you can see the dashboard.

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-web1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-web1.png" alt="SQL Example: WordCount web"/></a>

Click the job name: "default: insert into...", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-web2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-web2.png" alt="SQL Example: WordCount detail"/></a>

And run the following command to see the output result:

{% highlight bash %}
$ cat /tmp/output.csv
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-result.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-wordcount-result.png" alt="SQL Example: WordCount result"/></a>

Note that non-local file systems require a schema prefix, such as hdfs://.

### PV-UV Statistics (Input: Kafka)

This example reads the access data of the website in the following format from kafka, and gather statistics of pv-uv in real time.

Start SQL Client shell:

{% highlight bash %}
$ ./bin/sql-client.sh embedded
{% endhighlight %}

You can see the welcome message for flink sql client.

Paste the following sql ddl text into the shell. (For more information about sql ddl refer to [SQL]({{ site.baseurl }}/dev/table/sql.html) and [Supported DDL]({{ site.baseurl }}/dev/table/supported_ddl.html))

<b>Note</b>: Replace the **bootstrap.servers** and **group.id** with your own environment.

{% highlight bash %}
create table kafka_source (
  messageKey varbinary, 
  message varbinary, 
  topic varchar,
  `partition` int,
  `offset` bigint
) with (
  type = 'kafka010',   
  topic = 'pvuv_demo',
  bootstrap.servers = 'YOUR_BROKER_IP:YOUR_BROKER_PORT',
  `group.id` = 'kafka_consumer_demo_group'
);
{% endhighlight %}

Press 'Enter', then paste the following sql dml text. 
{% highlight bash %}
select
    date_format (visit_time, 'yyyy-MM-dd HH:mm') as `visit_time`,
    count (user_id) as pv,
    count (distinct user_id) as uv
from (
        select
            split_index (cast(message as varchar), ',', 0) as visit_time,
            split_index (cast(message as varchar), ',', 1) as user_id,
            split_index (cast(message as varchar), ',', 2) as visit_page,
            split_index (cast(message as varchar), ',', 3) as browser_type
        from
            kafka_source
    )
group by
    date_format (visit_time, 'yyyy-MM-dd HH:mm');
{% endhighlight %}

After press 'Enter' the sql will be submitted to the standalone cluster. Since there is no data in kafka topic at this time, no output.

Then run kafka-console-producer.sh script under your local kafka installation package: 
<b>Note</b>: Replace the **--broker-list** with your own environment.

{% highlight bash %}
$ ./bin/kafka-console-producer.sh --topic pvuv_demo --broker-list YOUR_BROKER_IP:YOUR_BROKER_PORT
1 >2018-10-16 09:00:00,1001,/page1,chrome
2 >2018-10-16 09:00:02,1001,/page2,safari
3 >2018-10-16 09:00:07,1005,/page1,safari
4 >2018-10-16 09:01:30,1001,/page1,chrome
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-input.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-input.png" alt="PV-UV Statistics (Input: Kafka) input"/></a>

You can see output like this:

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-output.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-output.png" alt="PV-UV Statistics (Input: Kafka) output"/></a>

Open [http://localhost:8081](http://localhost:8081) and you can see the dashboard.

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-web1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-web1.png" alt="SQL Example: PV-UV Statistics (Input: Kafka) web"/></a>

Click the job name: "default: select date_format...", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-web2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-kafka-pvuv-web2.png" alt="SQL Example: PV-UV Statistics (Input: Kafka) detail"/></a>

Animated demo of the example:
<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-demo-pv-uv-kafka.gif"><img class="offset" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-demo-pv-uv-kafka.gif" alt="Animated demo of the Flink SQL Client CLI running pv-uv sql on a cluster"  width="80%"/></a>

For more information please refer to [SQL]({{ site.baseurl }}/dev/table/sql.html) and [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html).

## Submit SQL Query Programmatically
SQL queries can be submitted using the `sqlQuery()` method of the TableEnvironment programmatically. 

StreamJoinSQLExample shows the usage of SQL Join on Stream Tables. It computes orders shipped within one hour. The algorithm works in two steps: First, join the Order table and the Shipment table on the orderId field. Second, filter the join result by createTime.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// set up the execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

DataStream<Order> order = env.fromElements(
    new Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
    new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
    new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
    new Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
    new Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8));
DataStream<Shipment> shipment = env.fromElements(
    new Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
    new Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
    new Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5));

// register the DataStreams under the name "t_order" and "t_shipment"
tEnv.registerDataStream("t_order", order, "createTime, unit, orderId, productId");
tEnv.registerDataStream("t_shipment", shipment, "createTime, orderId");

// run a SQL to get orders whose ship date are within one hour of the order date
Table table = tEnv.sqlQuery(
    "SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
        " FROM t_order AS o" +
        " JOIN t_shipment AS s" +
        "  ON o.orderId = s.orderId" +
        "  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR");

DataStream<Row> resultDataStream = tEnv.toAppendStream(table, Row.class);
resultDataStream.print();

// execute program
env.execute();
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/StreamJoinSQLExample.java  "StreamJoinSQLExample" %} implements the above described algorithm.

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// set up the execution environment
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

val t_order: DataStream[Order] = env.fromCollection(Seq(
  Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
  Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
  Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
  Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
  Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8)))

val t_shipment: DataStream[Shipment] = env.fromCollection(Seq(
  Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
  Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
  Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5)))

// register the DataStreams under the name "t_order" and "t_shipment"
tEnv.registerDataStream("t_order", t_order, 'createTime, 'unit, 'orderId, 'productId)
tEnv.registerDataStream("t_shipment", t_shipment, 'createTime, 'orderId)

// run a SQL to get orders whose ship date are within one hour of the order date
val result = tEnv.sqlQuery(
  "SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
    " FROM t_order AS o" +
    " JOIN t_shipment AS s" +
    "  ON o.orderId = s.orderId" +
    "  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR")

result.toAppendStream[Row].print()

// execute program
env.execute()

// user-defined pojo
case class Order(createTime: Timestamp, unit: Int, orderId: Long, productId: Long)

case class Shipment(createTime: Timestamp, orderId: Long)
    
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala/StreamJoinSQLExample.scala  "StreamJoinSQLExample.scala" %} implements the above described algorithm.

</div>
</div>

To run the StreamJoinSQLExample, issue the following command:

{% highlight bash %}
$ ./bin/flink run ./examples/table/StreamJoinSQLExample.jar
{% endhighlight %}

Open [http://localhost:8081](http://localhost:8081) and you can see the dashboard.

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-stream-join-sql.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-stream-join-sql.png" alt="SQL Example: Stream Join SQL web"/></a>

And run the following command to see the result:

{% highlight bash %}
$ tail -f ./log/flink-*-taskexecutor*.out
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-stream-join-sql-result.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/stream-sqlclient-example-stream-join-sql-result.png" alt="Stream SQL Example: Stream Join SQL result"/></a>

{% top %}
