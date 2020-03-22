---
title: "Query Configuration"
nav-parent_id: streaming_tableapi
nav-pos: 6
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

Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input. In many cases, continuous queries on streaming input are capable of computing accurate results that are identical to offline computed results. However, this is not possible in general case because continuous queries have to restrict the size of the state they are maintaining in order to avoid to run out of storage and to be able to process unbounded streaming data over a long period of time. As a result, a continuous query might only be able to provide approximated results depending on the characteristics of the input data and the query itself.

Flink's Table API and SQL interface provide parameters to tune the accuracy and resource consumption of continuous queries. The parameters are specified via a `TableConfig` object. The `TableConfig` can be obtained from the `TableEnvironment` and is passed back when a `Table` is translated, i.e., when it is [transformed into a DataStream]({{ site.baseurl }}/dev/table/common.html#convert-a-table-into-a-datastream-or-dataset) or [emitted via a TableSink](../common.html#emit-a-table).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// obtain query configuration from TableEnvironment
TableConfig tConfig = tableEnv.getConfig();
// set query parameters
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));

// define query
Table result = ...

// create TableSink
TableSink<Row> sink = ...

// register TableSink
tableEnv.registerTableSink(
  "outputTable",               // table name
  new String[]{...},           // field names
  new TypeInformation[]{...},  // field types
  sink);                       // table sink

// emit result Table via a TableSink
result.insertInto("outputTable");

// convert result Table into a DataStream<Row>
DataStream<Row> stream = tableEnv.toAppendStream(result, Row.class);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// obtain query configuration from TableEnvironment
val tConfig: TableConfig = tableEnv.getConfig
// set query parameters
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

// define query
val result: Table = ???

// create TableSink
val sink: TableSink[Row] = ???

// register TableSink
tableEnv.registerTableSink(
  "outputTable",                  // table name
  Array[String](...),             // field names
  Array[TypeInformation[_]](...), // field types
  sink)                           // table sink

// emit result Table via a TableSink
result.insertInto("outputTable")

// convert result Table into a DataStream[Row]
val stream: DataStream[Row] = result.toAppendStream[Row]

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
# use TableConfig in python API
t_config = TableConfig()
# set query parameters
t_config.set_idle_state_retention_time(timedelta(hours=12), timedelta(hours=24))

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, t_config)

# define query
result = ...

# create TableSink
sink = ...

# register TableSink
table_env.register_table_sink("outputTable",  # table name
                              sink)  # table sink

# emit result Table via a TableSink
result.insert_into("outputTable")

{% endhighlight %}
</div>
</div>

In the following we describe the parameters of the `TableConfig` and how they affect the accuracy and resource consumption of a query.

Idle State Retention Time
-------------------------

Many queries aggregate or join records on one or more key attributes. When such a query is executed on a stream, the continuous query needs to collect records or maintain partial results per key. If the key domain of the input stream is evolving, i.e., the active key values are changing over time, the continuous query accumulates more and more state as more and more distinct keys are observed. However, often keys become inactive after some time and their corresponding state becomes stale and useless.

For example the following query computes the number of clicks per session.

{% highlight sql %}
SELECT sessionId, COUNT(*) FROM clicks GROUP BY sessionId;
{% endhighlight %}

The `sessionId` attribute is used as a grouping key and the continuous query maintains a count for each `sessionId` it observes. The `sessionId` attribute is evolving over time and `sessionId` values are only active until the session ends, i.e., for a limited period of time. However, the continuous query cannot know about this property of `sessionId` and expects that every `sessionId` value can occur at any point of time. It maintains a count for each observed `sessionId` value. Consequently, the total state size of the query is continuously growing as more and more `sessionId` values are observed.

The *Idle State Retention Time* parameters define for how long the state of a key is retained without being updated before it is removed. For the previous example query, the count of a `sessionId` would be removed as soon as it has not been updated for the configured period of time.

By removing the state of a key, the continuous query completely forgets that it has seen this key before. If a record with a key, whose state has been removed before, is processed, the record will be treated as if it was the first record with the respective key. For the example above this means that the count of a `sessionId` would start again at `0`.

There are two parameters to configure the idle state retention time:
- The *minimum idle state retention time* defines how long the state of an inactive key is at least kept before it is removed.
- The *maximum idle state retention time* defines how long the state of an inactive key is at most kept before it is removed.

The parameters are specified as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

TableConfig tConfig = ...

// set idle state retention time: min = 12 hours, max = 24 hours
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

val tConfig: TableConfig = ???

// set idle state retention time: min = 12 hours, max = 24 hours
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

t_config = ...  # type: TableConfig

# set idle state retention time: min = 12 hours, max = 24 hours
t_config.set_idle_state_retention_time(timedelta(hours=12), timedelta(hours=24))

{% endhighlight %}
</div>
</div>

Cleaning up state requires additional bookkeeping which becomes less expensive for larger differences of `minTime` and `maxTime`. The difference between `minTime` and `maxTime` must be at least 5 minutes.

{% top %}
