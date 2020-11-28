---
title: "Dynamic Tables"
nav-parent_id: unbounded_data
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

SQL and the relational algebra have not been designed with streaming data in mind. As a consequence, there are few conceptual gaps between relational algebra (and SQL) and stream processing.

This page discusses these differences and explains how Flink can achieve the same semantics on unbounded data as a regular database engine on bounded data.


* This will be replaced by the TOC
{:toc}

Relational Queries on Unbounded Data
------------------------------------

First, let's try to understand the challenges processing unbounded data presents as compared to bounded data. 
In Flink, unbounded data is represented as data stream while bounded data is represented as batch table.

<table class="table table-bordered">
	<tr>
		<th>Relational Algebra / SQL</th>
		<th>Stream Processing</th>
	</tr>
	<tr>
		<td>Relations (or tables) are bounded (multi-)sets of tuples.</td>
		<td>A stream is an infinite sequences of tuples.</td>
	</tr>
	<tr>
		<td>A query that is executed on batch data (e.g., a table in a relational database) has access to the complete input data.</td>
		<td>A streaming query cannot access all data when it is started and has to "wait" for data to be streamed in.</td>
	</tr>
	<tr>
		<td>A batch query terminates after it produced a fixed sized result.</td>
		<td>A streaming query continuously updates its result based on the received records and never completes.</td>
	</tr>
</table>

Despite these challenges, processing streams with relational queries and SQL is not impossible. We take inspirationg from the *Materialized Views* feature of the relational database systems. A materialized view is defined as a SQL query, just like a regular virtual view. In contrast to a virtual view, a materialized view caches the result of the query such that the query does not need to be evaluated when the view is accessed. However, a common challenge for Materialized Views is to prevent the cache from serving outdated results when the base tables of its definition query are modified. Databases use *Eager View Maintenance* technique to update a materialized view as soon as its base tables are updated.

We can draw the following analogy between Eager view maintenance and Streaming SQL queries:

- A table in most of the databases as well as Flink is the result of a *stream* of `INSERT`, `UPDATE`, and `DELETE` DML statements, often called *changelog stream*.
- The result of Streaming SQL query is equivalent to a Materialized View.
- The SQL query continuously processes the changelog streams of the base tables to update the result stream.

Flink Table API defines these materialized view as *Dynamic tables*. Let's take a look at them in the next section.

Dynamic Tables &amp; Continuous Queries
---------------------------------------

*Dynamic tables* are the core concept of Flink's Table API and SQL to support queries on streaming data. In contrast to the static tables that represent batch data, dynamic tables change over time. However, they can be queried like static batch tables. A query on Dynamic Tables never terminates and continuously updates its result to reflect the changes on its input tables. These queries are known as *Continuous Queries*. If Dynamic Table is equivalent to a Materialized View then continuous queries are equivalent to the SQL query defined on the base table.

It is important to note that the result of a continuous query is always semantically equivalent to the result of the same query executed in batch mode on a snapshot of the input tables.

The following figure clearly visualizes the relationship bwteeen streams, dynamic tables, and  continuous queries:

<center>
<img alt="Dynamic tables" src="{{ site.baseurl }}/fig/table-streaming/stream-query-stream.png" width="80%">
</center>


1. A stream is converted into a dynamic table.
2. A continuous query is evaluated on the dynamic table yielding a new dynamic table.
3. The resulting dynamic table is converted back into a stream.

**Note:** Dynamic tables are foremost a logical concept. Dynamic tables may not necessarily (fully) materialize during query execution.

### Define a Dynamic Table

In order to process a stream with a relational query, it has to be converted into a `Table`. Conceptually, each record of the stream is interpreted as an `INSERT` modification on the resulting table. Essentially, we are building a table from an `INSERT`-only changelog stream.

The following figure visualizes how the stream of click event (left-hand side) is converted into a table (right-hand side). The resulting table is continuously growing as more records of the click stream are inserted.

<center>
<img alt="Append mode" src="{{ site.baseurl }}/fig/table-streaming/append-mode.png" width="60%">
</center>


All the examples in the next few sections use the `clicks` table that has the following schema:

{% highlight plain %}
[
  user:  VARCHAR,   // the name of the user
  cTime: TIMESTAMP, // the time when the URL was accessed
  url:   VARCHAR    // the URL that was accessed by the user
]
{% endhighlight %}

**Note:** A table which is defined on a stream is not materialized internally.

## Continuous Queries

As mentioned in the introduction, Continuous query is a query that never terminates and keeps updating its result table according to the changes on its input tables.

In the following we show two example queries on `clicks` table.

The first query is a simple `GROUP-BY COUNT` aggregation query. It groups the `clicks` table on the `user` field and counts the number of visited URLs. The following figure shows how the query is evaluated over time as the `clicks` table is updated with additional rows.

<center>
<img alt="Continuous Non-Windowed Query" src="{{ site.baseurl }}/fig/table-streaming/query-groupBy-cnt.png" width="90%">
</center>

When the query is started, the `clicks` table (left-hand side) is empty. The query starts to compute the result table, when the first row is inserted into the `clicks` table. The following updates occur in the result table after each new row in input table:

1. When the first row `[Mary, ./home]` is inserted, a new row `[Mary, 1]` is added to the result table.
2. On second row `[Bob, ./cart]`, the query inserts a new row `[Bob, 1]` in result.
3. The third row `[Mary, ./prod?id=1]` results in the update of the first row to `[Mary, 2]`.
4. The forth row `[Liz, ./home]` in the input adds a new row `[Liz, 1]` to the result.

The second query is similar to the first one but groups the `clicks` table in addition to the `user` attribute also on an [hourly tumbling window]({{ site.baseurl }}/dev/table/sql/queries.html#group-windows) before it counts the number of URLs (time-based computations such as windows are based on special [time attributes](time_attributes.html), which are discussed later.). This implies the updates for a particular hour are processed together while the updates for the next hour are treated as an entirely new set of input
Again, the figure shows the input and output at different points in time to visualize the changing nature of dynamic tables.

<center>
<img alt="Continuous Group-Window Query" src="{{ site.baseurl }}/fig/table-streaming/query-groupBy-window-cnt.png" width="100%">
</center>

As before, the input table `clicks` is shown on the left. The query continuously computes results every hour and updates the result table. The clicks table contains four rows with timestamps (`cTime`) between the first hour window `12:00:00` and `12:59:59`. The query computes two results rows in this window (one for each `user`) and appends them to the result table. For the next window between `13:00:00` and `13:59:59`, the `clicks` table contains three rows, which results in another two new rows in the result table. The result table is updated, as more rows are appended to `clicks` over time. The important point is once the window has passed in time, the data for that window is never updated.

Although the two example queries appear to be quite similar (both compute a grouped count aggregate), they differ in one important aspect:
- The first query updates previously emitted results, i.e., the changelog stream that defines the result table contains `INSERT` and `UPDATE` changes.
- The second query only appends to the result table, i.e., the changelog stream of the result table only consists of `INSERT` changes.

Whether a query produces an append-only table or an updated table has some implications:
- Queries that produce update changes usually have to maintain more state (see the following section).
- The conversion of an append-only table into a stream is different from the conversion of an updated table (see the [Table to Stream Conversion](#table-to-stream-conversion) section).

Table to Stream Conversion
--------------------------

A dynamic table can be continuously modified by `INSERT`, `UPDATE`, and `DELETE` changes just like a regular database table. It might be a table with a single row, which is constantly updated, an insert-only table without `UPDATE` and `DELETE` modifications, or anything in between.

When converting a dynamic table into a stream or writing it to an external system, these changes need to be encoded. Flink's Table API and SQL support three ways to encode the changes of a dynamic table:

* **Append-only stream:** A dynamic table that is only modified by `INSERT` changes can be converted into a stream just by emitting the inserted rows.

* **Retract stream:** A retract stream is a stream with two types of messages: 

  * **Add messages** - These messages represent a new row being added to the table. `INSERT` statements are encoded as Add messages.
  * **Retract messages**. The messages represents a row being deleted from the table. `DELETE` statement are encoded as Delete messages. 
      
`UPDATE` statements are encoded as a Retract message for the updated (previous) row and an Add message for the updating (new) row.
 
 The following figure visualizes the conversion of a dynamic table into a retract stream.

<center>
<img alt="Dynamic tables" src="{{ site.baseurl }}/fig/table-streaming/undo-redo-mode.png" width="85%">
</center>
<br><br>

* **Upsert stream:** An upsert stream is a stream with two types of messages - 

   * **Upsert messages** - These messsages encode `INSERT` and `UPDATE` changes. Encoding an `UPDATE` message is not possible unless the table has a (possibly composite) unique key.  The stream consuming operator needs to be aware of the unique key attribute in order to apply messages correctly.
   
   * **Delete messages** - `DELETE` changes as delete messages. The stream consuming operator needs to be aware of the unique key attribute in order to apply messages correctly. 
   
   The main difference between and upstert stream and a retract stream is that `UPDATE` changes are encoded with a single message and hence more efficient. The following figure visualizes the conversion of a dynamic table into an upsert stream.

<center>
<img alt="Dynamic tables" src="{{ site.baseurl }}/fig/table-streaming/redo-mode.png" width="85%">
</center>
<br><br>

The API to convert a dynamic table into a `DataStream` is discussed on the [Common Concepts]({{ site.baseurl }}/dev/table/common.html#convert-a-table-into-a-datastream) page. Please note that only Append and Retract streams are supported when converting a dynamic table into a `DataStream`. The `TableSink` interface to emit a dynamic table to an external system are discussed on the [TableSources and TableSinks](../sourceSinks.html#define-a-tablesink) page.

Challenges in Unbounded Data Processing
----------------------------------------

Most of the semantically valid queries can be evaluated as continuous queries on streams. However, there are some queries that are too expensive to compute, either due to the size of state that they need to maintain or because computing updates is too expensive.

- **State Size:** Continuous queries are evaluated on unbounded streams and are often supposed to run for weeks or months. Hence, the total amount of data that a continuous query processes can be very large. Queries that have to update previously emitted results need to maintain all emitted rows in order to be able to update them. For instance, the first example query needs to store the URL count for each user to be able to increase the count and sent out a new result when the input table receives a new row. If only registered users are tracked, the number of counts to maintain might not be too high. However, if non-registered users get a unique user name assigned, the number of counts to maintain would grow over time and might eventually cause the query to fail.

    {% highlight sql %}
    SELECT user, COUNT(url)
    FROM clicks
    GROUP BY user;
    {% endhighlight %}

- **Computing Updates:** Some queries require to recompute and update a large fraction of the emitted result rows even if only a single input record is added or updated. Clearly, such queries are not well suited to be executed as continuous queries. An example is the following query which computes for each user a `RANK` based on the time of the last click. As soon as the `clicks` table receives a new row, the `lastAction` of the user is updated and a new rank must be computed. However since two rows cannot have the same rank, all lower ranked rows need to be updated as well.

    {% highlight sql %}
    SELECT user, RANK() OVER (ORDER BY lastAction)
    FROM (
      SELECT user, MAX(cTime) AS lastAction FROM clicks GROUP BY user
    );
    {% endhighlight %}

Tuning Query State
----------------------

As discussed in previous section, for some continuous queries you have to limit the size of the state they are maintaining in order to avoid to run out of storage. It depends on the characteristics of the input data and the query itself whether you need to limit the state size and if how it affects the accuracy of the computed results.

Flink's Table API and SQL interface provide parameters to tune the accuracy and resource consumption of continuous queries. The parameters are specified via a `TableConfig` object, which can be obtained from the `TableEnvironment`.

Currently, you can tune the Idle state retention time in Flink. By removing the state of a key, the continuous query completely forgets that it has seen this key before. If a record with a key, whose state has been removed before, is processed, the record will be treated as if it was the first record with the respective key. e.g. In the queries mentioned in the [Challenges in Unbounded Data Processing](#challenges-in-unbounded-data-processing) section, we can allow flink to forget about an `user` after 1 hour. If a new event, arrives for the same user after the state has been cleared up, the count will be initiated to `0`

There are two parameters to configure the idle state retention time:
- The **minimum idle state retention time** defines how long the state of an inactive key is at least kept before it is removed.
- The **maximum idle state retention time** defines how long the state of an inactive key is at most kept before it is removed.



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
result.executeInsert("outputTable");

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
result.executeInsert("outputTable")

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

Cleaning up state requires additional bookkeeping which becomes less expensive for larger differences of `minTime` and `maxTime`. The difference between `minTime` and `maxTime` must be at least 5 minutes.

{% top %}
